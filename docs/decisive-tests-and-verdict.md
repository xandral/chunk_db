# Test decisivi e verdetto sul progetto

Data: 2026-08-16. Questa nota descrive i test eseguiti dopo l'introduzione
della griglia adattiva multidimensionale e separa i risultati osservati dalle
ipotesi ancora da verificare.

## Risposta breve

L'idea non è un buco nell'acqua. Il routing diretto per coordinate produce un
vantaggio reale sulle query selettive allineate alle dimensioni, e gli split
locali su hash/range eliminano gli overflow senza dover raffinare tutta la
griglia. Il progetto ha quindi valore come storage embedded sperimentale o
come layout/indice specializzato.

Il risultato non giustifica invece, oggi, la definizione di HTAP general
purpose. Un Parquet unico, ordinato e con row group capienti resta migliore
per scansioni temporali, larghe e complete. Inoltre uno split-on-overflow non
può correggere celle di base che nascono già sottodimensionate: per UUID e
Zipf la griglia full resta composta per il 92-100% da file piccoli, pur essendo
molto meno frammentata del reticolo fisso.

La conclusione architetturale è precisa: il prossimo passo utile è separare la
cella logica indirizzabile dal segmento fisico. Più celle sparse devono poter
condividere un segmento capiente, immutabile e versionato.

## Verifica di correttezza

Comando finale: `cargo test`.

| Gruppo | Test | Esito |
|---|---:|---:|
| Unit test | 74 | 74 verdi |
| Griglia adattiva multidimensionale | 4 | 4 verdi |
| Regressioni decisive | 6 | 6 verdi |
| Integrazione preesistente | 18 | 18 verdi |
| Totale | **102** | **102 verdi, 0 ignorati** |

### Regressioni decisive

File: [`tests/decisive_correctness_test.rs`](../tests/decisive_correctness_test.rs).

| Test | Rischio coperto | Garanzia ottenuta |
|---|---|---|
| Seconda dimensione hash | Il pruner usava l'indice della prima dimensione | Ogni predicato usa il proprio asse |
| Cache e snapshot | Uno snapshot vecchio poteva leggere una cache futura | Una cache è riusata solo se compatibile con lo snapshot |
| Column group e patch | Patch full-schema applicata a un batch proiettato | La patch viene proiettata sullo schema fisico |
| Insert concorrenti | Due read/merge/write potevano perdere un batch | La sequenza è serializzata per tabella |
| Insert durante compaction | La riscrittura poteva cancellare un insert concorrente | Compaction e insert condividono lo stesso lock |
| Update della chiave di partizione | Una riga poteva restare nella vecchia cella | Lo spostamento è rifiutato esplicitamente; serve delete+insert |

### Griglia multidimensionale

File: [`tests/adaptive_multidimensional_test.rs`](../tests/adaptive_multidimensional_test.rs).

| Test | Cosa verifica |
|---|---|
| Split hash locale | Split, column group, reopen, nuovi insert, delete, compact e merge dei fratelli |
| Split range locale | Routing e pruning esatti al livello locale, anche dopo reopen |
| Layout Parquet | Ordinamento fisico e row group limitati alla dimensione configurata/8.192 righe |
| Writer concorrenti | Un'unica topologia persistente senza righe perse o duplicate |

I test unitari coprono inoltre: formule annidate hash/range/row, valori range
negativi, snapshot deterministici della `DimensionMap`, migrazione delle
coordinate legacy, nomi file con livelli locali e invarianti di split/merge.

## Benchmark decisivo

Runner: [`examples/decisive_layout_benchmark.rs`](../examples/decisive_layout_benchmark.rs).
I CSV grezzi `decisive_quick_phase3.csv` e `decisive_full_phase3.csv` sono
artifact locali intenzionalmente non versionati; i comandi in fondo al
documento li rigenerano. Le tabelle sotto registrano i risultati della prova
eseguita il 2026-08-16.

Ogni profilo incrocia due distribuzioni (uniforme e Zipf 1,15), due strategie
di ID (timestamp ordinato e UUID pseudocasuale), tre layout e cinque query. I
CSV contengono 60 righe ciascuno; tutte le righe restituite coincidono con il
risultato atteso.

| Profilo | Righe | Colonne valore | Sensori | Bucket hash base | Target | Misure |
|---|---:|---:|---:|---:|---:|---:|
| quick | 60.000 | 6 | 32 | 8 | 3.000 | 3 |
| full | 500.000 | 32 | 512 | 32 | 20.000 | 7 |

I tre layout sono il reticolo rettangolare fisso, la griglia adattiva
multidimensionale e un singolo Parquet ordinato per timestamp con row group al
target. Le latenze sono warm e sulla stessa macchina; sono un confronto del
layout, non un confronto definitivo tra motori SQL.

### Profilo full: layout e scansione completa

`piccoli` significa meno di un quarto del target. `split r/h/rg` conta i nodi
interni persistenti sugli assi row, hash e range. Le latenze sono p50 in ms.

| Distribuzione / ID | Layout | File / row group | Piccoli | Split r/h/rg | Disk/live MiB | Full scan |
|---|---|---:|---:|---:|---:|---:|
| Uniforme / timestamp | fisso | 832 file | 100% | 0/0/0 | 1.781/1.781 | 497,004 |
| | adattivo | 68 file | **4,4%** | 1/4/0 | 594/298 | 250,222 |
| | Parquet | 1 file / 25 RG | 0% | - | 113/113 | **198,804** |
| Uniforme / UUID | fisso | 5.600 file | 100% | 0/0/0 | 11.379/11.379 | 2.695,057 |
| | adattivo | 224 file | 100% | 0/0/0 | 571/571 | 316,871 |
| | Parquet | 1 file / 25 RG | 0% | - | 113/113 | **203,162** |
| Zipf / timestamp | fisso | 832 file | 100% | 0/0/0 | 1.790/1.790 | 537,288 |
| | adattivo | 256 file | 94,1% | 7/0/0 | 1.641/649 | 341,204 |
| | Parquet | 1 file / 25 RG | 0% | - | 113/113 | **198,549** |
| Zipf / UUID | fisso | 5.600 file | 100% | 0/0/0 | 11.380/11.380 | 2.715,536 |
| | adattivo | 224 file | 92,0% | 0/0/0 | 607/607 | 314,167 |
| | Parquet | 1 file / 25 RG | 0% | - | 113/113 | **198,142** |

Il caso uniforme/timestamp dimostra che la nuova implementazione opera
davvero su più dimensioni: quattro split hash locali e uno row portano i file
piccoli dal 100% al 4,4%. Il rapporto `disk/live` mostra però anche le vecchie
versioni create dalle riscritture prima della garbage collection.

Nei casi UUID full, i 32 bucket hash base moltiplicati per i 7 bucket range
producono 224 celle. Ogni cella è già sotto `max_cell_rows`, quindi non esiste
un overflow che possa attivare uno split. Questo è il motivo per cui la
percentuale di file piccoli resta alta: non è un errore del nuovo routing, è
il limite matematico di una strategia che sa dividere ma non sa aggregare
celle base indipendenti.

### Query selettive

Alcuni risultati p50 del profilo full uniforme:

| ID | Query | Adattivo | Parquet ordinato | Migliore |
|---|---|---:|---:|---:|
| timestamp | solo sensore | 0,757 ms | 16,721 ms | adattivo 22,1× |
| timestamp | sensore + tempo 1% | 0,442 ms | 1,314 ms | adattivo 3,0× |
| UUID | solo sensore | 1,408 ms | 12,961 ms | adattivo 9,2× |
| UUID | sensore + tempo 1% | 0,256 ms | 1,351 ms | adattivo 5,3× |
| timestamp | tempo 1% | 14,559 ms | 1,106 ms | Parquet 13,2× |
| UUID | tempo 10% | 99,863 ms | 26,038 ms | Parquet 3,8× |

Il routing diretto ha quindi uno sweet spot reale: uguaglianze hash e
congiunzioni molto selettive. Per range scan e full scan vince il layout
sequenziale con pochi file.

## Cosa deduciamo

1. La griglia dinamica ora esiste su row, hash e range; non è più il vecchio
   split soltanto sulla row dimension.
2. La scelta data-driven dell'asse funziona: sceglie hash nel caso uniforme,
   row o range quando risultano più bilanciati e limita la riscrittura alla
   cella logica quando usa un asse locale.
3. Il reticolo fisso non è un default sostenibile: nel caso peggiore crea
   5.600 file e oltre 11 GiB per 500.000 righe che un Parquet contiene in
   circa 113 MiB.
4. Lo split-on-overflow risolve celle troppo grandi, non celle già troppo
   piccole. Il merge implementato ricompone solo fratelli generati da uno
   split; non può fondere bucket base arbitrari.
5. Un file per coordinata conserva l'accesso diretto ma rende antagoniste
   granularità logica e dimensione fisica. È questo, non l'idea dei
   rettangoli, il vincolo da rimuovere.
6. Il write path è corretto ma ancora costoso: merge-on-write, split e
   compaction riscrivono file; il lock per tabella evita lost update ma
   serializza le scritture.

## Decisione

**Go** come progetto di ricerca e come possibile storage embedded per query
selettive su dimensioni note. **No-go**, allo stato attuale, come sostituto
general purpose di DuckDB/ClickHouse o come HTAP con MVCC completo.

Il prossimo esperimento dovrebbe introdurre una directory
`cella logica -> segmento fisico/versione`, packing di più celle per target di
capacità e segmenti/delta immutabili pubblicati da un manifest atomico. Il gate
successivo è:

- meno del 10% di unità fisiche piccole anche con UUID + Zipf;
- mantenimento del vantaggio sulle congiunzioni selettive;
- write amplification sotto 1,5×;
- correttezza crash/snapshot su insert, split, merge e compaction concorrenti.

## Limiti della prova

- Misure warm e dataset in RAM; niente cold cache, dataset oltre RAM o tail
  latency sotto carico misto.
- Il baseline è un lettore Arrow/Parquet semplice, non DuckDB.
- I Bloom filter sono scritti nei chunk, ma il direct executor non li consulta
  ancora; oggi usa coordinate e statistiche dei row group.
- Il benchmark usa un solo column group, quindi non quantifica separatamente
  il costo del join verticale.
- L'MVCC è completo per le patch, non per tutte le versioni dei file base; una
  riscrittura del base non garantisce uno snapshot storico generale.

## Comandi riproducibili

```bash
cargo test
cargo test --test decisive_correctness_test
cargo test --test adaptive_multidimensional_test

cargo run --release --example decisive_layout_benchmark -- \
  --profile quick \
  --output benchmarks/results/decisive_quick_phase3.csv

cargo run --release --example decisive_layout_benchmark -- \
  --profile full \
  --output benchmarks/results/decisive_full_phase3.csv
```
