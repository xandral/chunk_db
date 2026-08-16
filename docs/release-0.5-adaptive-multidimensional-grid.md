# Release 0.5 — Griglia adattiva multidimensionale

Data: 2026-08-16. Implementazione della Phase 3 descritta in
[`architecture-evolution.md`](architecture-evolution.md).

## Risultato

La griglia non si raffina più soltanto lungo `__row_id`. Ogni foglia row ha
una topologia locale persistente che può dividersi lungo una singola
dimensione hash o range. Lo split usa sempre formule deterministiche, quindi
il routing resta diretto: il catalogo memorizza soltanto quali celle sono nodi
interni e quale asse è stato scelto.

```text
row LevelMap
  └─ row leaf
       └─ DimensionMap: hash[0]
            ├─ leaf
            └─ range[0]
                 ├─ leaf -> Parquet, column group 0..N
                 └─ leaf -> Parquet, column group 0..N
```

La coordinata fisica è ora:

```rust
ChunkCoordinate {
    row_bucket: u64,
    level: u16,
    col_group: u16,
    hash_buckets: Vec<u64>,
    range_buckets: Vec<u64>,
    hash_levels: Vec<u16>,
    range_levels: Vec<u16>,
}
```

`CellCoordinate` rappresenta la stessa foglia senza `col_group`, cioè l'unità
logica su cui avvengono routing, split e merge.

## Routing e formule

Il router sceglie prima la foglia row tramite `LevelMap`, calcola i bucket
hash/range a livello zero e poi scende nella `DimensionMap`. Ogni nodo aumenta
il livello di un solo asse.

- Hash: `xxh3(raw) mod (base_buckets * 2^level)`.
- Range: `floor(value * 2^level / chunk_size)`, con aritmetica `i128` e
  divisione euclidea per i negativi.
- Row: formula preesistente `floor(row_id * 2^level / chunk_rows)` in `u128`.

Le liste `hash_levels` e `range_levels` sono incluse nei nomi file solo quando
contengono valori non nulli (`_hl...`, `_rgl...`). Coordinate e filename legacy
senza questi campi vengono caricati come livello zero; le vecchie chiavi
catalogo vengono migrate quando sono riscritte.

## Scelta e commit dello split

Quando una foglia supera `max_cell_rows`, il writer misura la distribuzione
reale lungo tutti gli assi disponibili. Per ogni candidato calcola il figlio
più grande e lo sbilanciamento; guarda anche oltre livelli intermedi vuoti,
necessario per valori concentrati sul bordo di una cella molto grossolana.

Un asse hash/range locale viene preferito se il suo figlio maggiore è entro il
10% del miglior split row. In questo modo, a qualità simile, viene riscritto un
solo rettangolo invece di tutte le combinazioni della foglia row. I pareggi
sono deterministici: range, hash, row, poi indice della dimensione.

Lo split locale:

1. legge tutte le column group della sola `CellCoordinate`;
2. ricostruisce il batch logico e calcola i due figli;
3. scrive ogni column group dei figli con una nuova versione;
4. pubblica figli, rimuove il padre e salva la nuova `DimensionMap` in un
   singolo batch sled;
5. aggiorna la mappa in memoria e invalida la cache interessata.

Uno split row resta globale per la foglia row. Copia nei due figli l'eventuale
albero hash/range locale e usa una vista transitoria padre+figli fino alla
barriera del `LevelMap`, evitando routing su una topologia parziale.

## Merge e manutenzione

`ChunkDb::rebalance(table)` ricompone fratelli hash/range sottodimensionati.
`ChunkDb::compact(table)` applica prima le patch e poi invoca lo stesso
rebalance. Il merge non vive nell'hot path degli insert, per evitare
oscillazioni split/merge.

Il merge è ammesso solo quando:

- entrambi i rami presenti sono foglie immediate dello stesso split;
- nessun figlio è a sua volta un nodo interno;
- la cella non ha patch pendenti;
- almeno un figlio è sotto `max_cell_rows / 4`;
- la somma non supera `max_cell_rows`.

Il merge non attraversa i confini della griglia base: non può aggregare due
bucket livello zero che non derivano dallo stesso split.

Le API `adaptive_grid_stats()` e `rebalance()` espongono rispettivamente la
forma della griglia e il lavoro di manutenzione eseguito.

## Layout dentro il file

Ogni riscrittura fisica passa da `write_table_parquet`:

- ordinamento per dimensioni range, poi hash, poi `__row_id`;
- row group limitati dal target e comunque a un massimo di 8.192 righe;
- Bloom filter Parquet sulle dimensioni hash presenti e su `__row_id`;
- compressione Snappy.

Il direct executor usa già le statistiche min/max dei row group. I Bloom
filter vengono persistiti ma non sono ancora letti dal query path.

## Correttezza e concorrenza

- Insert, compaction e rebalance condividono un lock per tabella sulla
  sequenza read/merge/write/catalog: niente lost update, con il costo di
  serializzare i writer.
- Il catalogo pubblica split e merge atomici insieme alla mappa persistente.
- Patch e cache sono indirizzate con tutti i livelli rilevanti.
- Un update che cambierebbe cella hash/range viene rifiutato con un errore
  esplicito; la rilocazione atomica resta un lavoro futuro.
- Configurazioni invalide, tipi non supportati, dimensioni nullable, nomi e
  column group inconsistenti falliscono prima di scrivere dati.

## Test e risultato sperimentale

La suite finale contiene 102 test attivi. I quattro scenari dedicati coprono
split hash, split range, reopen, column group, merge dopo delete/compact,
ordinamento/row group e writer concorrenti. Il benchmark quick/full aggiunge
120 misure validate contro il conteggio atteso.

Il risultato completo e il verdetto sono in
[`decisive-tests-and-verdict.md`](decisive-tests-and-verdict.md). Il dato più
importante è duplice:

- uniforme + timestamp: gli split multidimensionali portano i file piccoli
  dal 100% al 4,4%;
- UUID/Zipf: se i bucket base producono già celle sotto soglia, nessuno split
  può ricomporle e il 92-100% resta piccolo.

## Confine architetturale emerso

Phase 3 completa la parte di raffinamento prevista, ma mostra che una griglia
capace solo di dividere non basta. `un file = una coordinata` lega una buona
granularità logica a una cattiva granularità fisica nei dati sparsi.

La fase successiva non dovrebbe aggiungere altre euristiche di split. Dovrebbe
introdurre segmenti fisici capienti che contengono più celle logiche, una
directory cella-segmento e un manifest MVCC atomico con delta immutabili. In
questo modo il routing diretto rimane, mentre file size, compaction e versioni
diventano proprietà del livello fisico.
