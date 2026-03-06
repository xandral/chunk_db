# Guida al Training Loop PyTorch per LLM

> Riferimento per il notebook `01_pretraining_tinystories.ipynb`

---

## Indice

1. [Uno step di training da zero](#1-uno-step-di-training-da-zero)
2. [I tre difetti del loop base](#2-i-tre-difetti-del-loop-base)
   - [Weight Decay](#difetto-1--i-pesi-crescono-troppo--weight-decay)
   - [Gradient Clipping](#difetto-2--i-gradienti-esplodono--gradient-clipping)
   - [Gradient Accumulation](#difetto-3--il-batch-non-ci-sta-in-memoria--gradient-accumulation)
3. [Adam vs AdamW](#3-adam-vs-adamw)
4. [Mixed Precision (FP16)](#4-mixed-precision-fp16)
5. [Il codice del notebook spiegato riga per riga](#5-il-codice-del-notebook-spiegato-riga-per-riga)
   - [Setup prima del loop](#51-setup-prima-del-loop)
   - [Inizio del loop: learning rate scheduling](#52-inizio-del-loop-learning-rate-scheduling)
   - [Gradient accumulation](#53-gradient-accumulation)
   - [Clipping e optimizer step](#54-clipping-e-optimizer-step)
   - [Logging e checkpoint](#55-logging-e-checkpoint)
6. [Schema visivo di uno step completo](#6-schema-visivo-di-uno-step-completo)

---

## 1. Uno step di training da zero

Un modello neurale impara modificando i suoi pesi **W** in modo da ridurre l'errore sulle predizioni. Un singolo step ha **3 operazioni distinte** con nomi precisi:

```
loss.backward()       ← "il backward"  — calcola i gradienti
optimizer.step()      ← "lo step"      — aggiorna i pesi
optimizer.zero_grad() ← "zero grad"    — pulizia per il prossimo step
```

In codice minimale:

```python
# 1. Forward: il modello predice
output = model(input)

# 2. Loss: misura quanto la predizione era sbagliata
loss = cross_entropy(output, target)

# 3. Backward: calcola i gradienti e li scrive in W.grad per ogni peso
loss.backward()

# 4. Optimizer step: legge W.grad e aggiorna i pesi
optimizer.step()

# 5. Pulizia: azzera W.grad per il prossimo step
optimizer.zero_grad()
```

### Il backward: cosa fa esattamente

`loss.backward()` **non tocca i pesi**. Scrive solo un numero in `W.grad` per ogni parametro del modello.

```python
# Prima del backward:
W = 0.73      # il peso
W.grad = None # nessun gradiente ancora

loss.backward()

# Dopo il backward:
W = 0.73      # il peso NON e' cambiato
W.grad = -0.12 # ora sappiamo in che direzione muoverlo
```

Come calcola questi numeri? Usa la **chain rule** (regola della catena). Parte dalla loss e risale il grafo computazionale layer per layer, moltiplicando derivate all'indietro:

```
loss
  ↑
layer 24  →  grad_24 = d(loss)/d(W_24)
  ↑
layer 23  →  grad_23 = grad_24 × d(output_24)/d(W_23)
  ↑
layer 22  →  grad_22 = grad_23 × d(output_23)/d(W_22)
  ↑
  ...
layer 1   →  grad_1  = grad_2  × d(output_2)/d(W_1)
```

I gradienti si **propagano all'indietro** — da qui il nome backpropagation. Ogni `W.grad` e' uno scalare della stessa forma del peso stesso (un tensore `(512, 512)` ha un `.grad` di forma `(512, 512)`).

### L'optimizer step: cosa fa esattamente

`optimizer.step()` legge i gradienti da `W.grad` e aggiorna i pesi. E' qui che i pesi cambiano davvero.

```python
# Prima dello step:
W = 0.73
W.grad = -0.12

optimizer.step()   # W = W - lr * grad  (semplificato, AdamW fa di piu')

# Dopo lo step:
W = 0.73 - 0.001 * (-0.12) = 0.73012  # il peso e' cambiato
W.grad = -0.12                          # .grad non viene toccato dallo step
```

### Cosa e' un gradiente?

Il **gradiente** di un peso e' un numero che risponde alla domanda:

> *"Se aumento questo peso di un piccolo epsilon, la loss aumenta o diminuisce, e di quanto?"*

- Gradiente **positivo** → aumentare il peso peggiora la loss → diminuiscilo
- Gradiente **negativo** → aumentare il peso migliora la loss → aumentalo
- Gradiente **vicino a zero** → quel peso e' gia' vicino all'ottimo

```
W_nuovo = W_vecchio - learning_rate * gradiente
```

### Next-token prediction: label = input shiftato

Nel pretraining di un LLM, il modello predice il prossimo token. La label e' semplicemente l'input spostato di una posizione:

```
Input:  [Il, gatto, mangia, il]
Label:  [gatto, mangia, il, pesce]
         ↑
         il modello doveva predire "gatto" dato "Il"
         poi "mangia" dato "Il gatto"
         ecc.
```

In codice:
```python
input_ids = batch['input_ids']
labels = input_ids.clone()  # stessa sequenza, HuggingFace calcola lo shift internamente
```

---

## 2. I tre difetti del loop base

Il loop di 4 righe sopra funziona, ma ha tre problemi pratici nel training di LLM.

---

### Difetto 1 — I pesi crescono troppo → Weight Decay

#### Il problema

Immagina di trainare su frasi come *"il gatto mangia il pesce"*. Il modello impara che la parola "gatto" e' molto importante per predire "mangia" e aumenta enormemente il peso associato. Funziona sul training set, ma poi generalizza male su frasi nuove che non ha mai visto (overfitting).

In generale: ottimizzando solo la loss, i pesi tendono a crescere senza limite e a diventare "troppo specifici" per i dati di training.

#### La soluzione

Weight decay aggiunge una piccola penalita' proporzionale alla dimensione dei pesi ad ogni step:

```
W_nuovo = W - lr * gradiente - lr * λ * W
                                ↑
                          la penalita' (λ ≈ 0.01-0.1)
```

Ad ogni step, i pesi vengono "schiacciati" leggermente verso zero. I pesi grandi vengono penalizzati di piu'. Questo forza il modello a distribuire l'informazione su tanti pesi piccoli invece di concentrarla su pochi pesi enormi → **migliore generalizzazione**.

#### AdamW vs Adam

La differenza tra Adam e AdamW sta proprio qui:
- **Adam**: il weight decay viene aggiunto al gradiente prima dell'aggiornamento adattivo → interagisce con la stima della varianza del gradiente in modo indesiderato
- **AdamW**: il weight decay viene applicato direttamente ai pesi, separatamente dall'aggiornamento basato sul gradiente → matematicamente piu' corretto

```python
optimizer = torch.optim.AdamW(params, lr=1e-4, weight_decay=0.1)
#                                                ↑ λ
```

#### Quali parametri penalizzare?

**Bias e layer norm non vengono penalizzati.** Perche'?
- Il **bias** e' un offset, non una "dimensione" del modello. Penalizzarlo lo spingerebbe verso zero senza motivo.
- I pesi di **layer norm** scalano e shiftano le attivazioni — se li schiacciassimo verso zero il layer norm smetterebbe di funzionare.

Ecco perche' nel codice li separiamo in due gruppi:

```python
no_decay = ["bias", "layernorm", "layer_norm", "rmsnorm"]

optimizer_grouped_params = [
    # Gruppo 1: pesi normali → con weight decay
    {"params": [p for n, p in model.named_parameters()
                if not any(nd in n.lower() for nd in no_decay)],
     "weight_decay": WEIGHT_DECAY},

    # Gruppo 2: bias e norm → senza weight decay
    {"params": [p for n, p in model.named_parameters()
                if any(nd in n.lower() for nd in no_decay)],
     "weight_decay": 0.0},
]
```

---

### Difetto 2 — I gradienti esplodono → Gradient Clipping

#### Il problema

A volte il backward produce gradienti enormi — ad esempio se il modello ha fatto una predizione catastroficamente sbagliata su un batch anomalo. Un gradiente grandissimo significa un aggiornamento dei pesi grandissimo, che puo' "rompere" il modello in un solo step.

```
Senza clipping:
  gradiente = 10000
  W = W - 0.001 * 10000 = W - 10  ← pesi distrutti in un colpo solo

Con clipping (max_norm = 1.0):
  gradiente ridimensionato → aggiornamento controllato
```

#### La soluzione

Il gradient clipping calcola la **norma L2** di tutti i gradienti del modello messi insieme (come la lunghezza di un vettore nello spazio N-dimensionale dei pesi), e se supera la soglia li ridimensiona proporzionalmente — mantenendo la direzione, cambiando solo la lunghezza.

```
norma = sqrt(grad_1² + grad_2² + ... + grad_N²)

se norma > MAX_GRAD_NORM:
    grad_i = grad_i * (MAX_GRAD_NORM / norma)  per ogni i
```

```python
torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
```

**Ordine obbligatorio**: va fatto dopo `backward()` e prima di `optimizer.step()`. Se fatto in ordine sbagliato, si clippano i gradienti sbagliati.

---

### Difetto 3 — Il batch non ci sta in memoria → Gradient Accumulation

#### Il problema

Un batch grande e' meglio: stima del gradiente piu' precisa, training piu' stabile, meno rumore. Ma la GPU ha memoria limitata. Con un modello da 1B parametri in FP16 e sequenze da 512 token, forse riusciamo a mettere solo 4-8 esempi alla volta.

#### La soluzione

I gradienti si **accumulano** — il backward somma i gradienti al buffer esistente invece di sovrascriverlo. Possiamo quindi fare piu' forward+backward prima di fare lo step dell'optimizer.

```
Senza accumulation (batch=32):
  forward(32 esempi) → backward → step
  ← serve memoria per 32 esempi contemporaneamente

Con accumulation (micro_batch=4, steps=8):
  forward(4) → backward → buffer += grad_0
  forward(4) → backward → buffer += grad_1
  ...         (8 volte in totale)
  step  ← usa gradienti equivalenti a 32 esempi
  zero_grad  ← ORA azzeriamo il buffer
```

```python
for i, batch in enumerate(dataloader):
    loss = model(batch).loss
    loss = loss / ACCUMULATION_STEPS  # ← IMPORTANTE: normalizza
    loss.backward()                   # ← accumula, non azzera

    if (i + 1) % ACCUMULATION_STEPS == 0:
        optimizer.step()
        optimizer.zero_grad()         # ← azzera solo ora
```

#### Perche' dividere la loss?

Stiamo sommando i gradienti di `N` micro-batch. Senza la divisione, il gradiente finale sarebbe `N` volte piu' grande di quello che otterremmo con un batch grande vero. La divisione per `ACCUMULATION_STEPS` li riporta alla scala corretta.

Matematicamente: vogliamo la media dei gradienti, non la somma.

---

## 3. Adam vs AdamW

### Come funziona Adam

Adam non aggiorna i pesi con il gradiente puro. Mantiene due **medie mobili** per ogni peso:

```
m = media mobile dei gradienti          (momento 1° ordine — "direzione")
v = media mobile dei gradienti²         (momento 2° ordine — "varianza")
```

Ad ogni step:
```
m = β1 * m + (1 - β1) * grad          # aggiorna la media della direzione
v = β2 * v + (1 - β2) * grad²         # aggiorna la media della varianza

W = W - lr * m / (sqrt(v) + ε)        # aggiornamento adattivo
```

Il termine `m / sqrt(v)` e' la parte intelligente: divide il gradiente per la sua deviazione standard stimata. Questo significa che:
- Se un peso ha gradienti **grandi e costanti** → `v` grande → passo piccolo
- Se un peso ha gradienti **piccoli e rari** → `v` piccolo → passo grande

Ogni peso ha il suo learning rate effettivo. E' questo che rende Adam molto migliore di SGD su dati sparsi come il testo.

### Il problema: weight decay dentro Adam

Con Adam classico, il weight decay veniva aggiunto al gradiente **prima** dell'aggiornamento adattivo:

```
grad_con_wd = grad + λ * W      ← weight decay mescolato al gradiente

# poi Adam fa l'aggiornamento adattivo su grad_con_wd:
m = β1 * m + (1 - β1) * grad_con_wd
v = β2 * v + (1 - β2) * grad_con_wd²   ← v inquinato da λW

W = W - lr * m / (sqrt(v) + ε)
```

Il problema: `grad_con_wd` entra dentro `v`, quindi la stima della varianza viene **inquinata** dal termine di weight decay. Il risultato e' che il peso viene scalato per `1/sqrt(v)` — e questo scala anche la penalita' del weight decay in modo imprevedibile.

In pratica: il weight decay effettivo dipende da quanto e' stato attivo quel peso nei passi recenti:
- Pesi degli embedding (aggiornati raramente, `v` piccolo) → Adam penalizza tanto il weight decay
- Pesi dell'attenzione (aggiornati spesso, `v` grande) → Adam penalizza poco il weight decay

Il weight decay con Adam classico **non funziona come dovrebbe**.

### La soluzione: AdamW

AdamW (Loshchilov & Hutter, 2017) separa i due termini. Il weight decay viene applicato **direttamente sui pesi**, dopo e separatamente dall'aggiornamento adattivo:

```
# Prima: aggiornamento adattivo puro (solo gradiente, senza weight decay)
m = β1 * m + (1 - β1) * grad
v = β2 * v + (1 - β2) * grad²
W = W - lr * m / (sqrt(v) + ε)

# Poi: weight decay applicato direttamente, separato
W = W - lr * λ * W
```

Che si semplifica in:
```
W = W * (1 - lr * λ) - lr * m / (sqrt(v) + ε)
```

Ora il weight decay non interagisce con `v`. Ogni peso viene schiacciato verso zero **sempre della stessa quantita' relativa**, indipendentemente da quanto sia stato attivo.

### Il confronto visivo

```
ADAM (sbagliato):
  grad → [+ λW] → grad_wd → Adam adaptive update → ΔW
                                  ↑
                         v viene inquinato da λW
                         → weight decay scalato in modo imprevedibile

ADAMW (corretto):
  grad →          → Adam adaptive update → ΔW_adam  ┐
                                                     +→ W_nuovo
  W    → [* λ]   →          ΔW_wd (separato, pulito) ┘
```

### Perche' tutti i LLM usano AdamW

Con AdamW tutti i pesi vengono penalizzati in modo uniforme e prevedibile, indipendentemente dalla loro frequenza di aggiornamento. Per questo **tutti i LLM moderni usano AdamW** — mai Adam classico. In PyTorch:

```python
# MAI usare questo per LLM:
optimizer = torch.optim.Adam(params, lr=1e-4, weight_decay=0.1)

# SEMPRE questo:
optimizer = torch.optim.AdamW(params, lr=1e-4, weight_decay=0.1)
```

Le due righe sembrano identiche ma producono comportamenti molto diversi quando il weight decay e' diverso da zero.

---

## 4. Mixed Precision (FP16)

### I formati numerici

I numeri in memoria possono essere rappresentati con diversi livelli di precisione:

```
FP32 (32 bit): range ±3.4e38, precisione ~7 cifre decimali  ← default PyTorch
FP16 (16 bit): range ±65504,  precisione ~3.3 cifre         ← meta' della memoria
BF16 (16 bit): range ±3.4e38, precisione ~2.4 cifre         ← stesso range di FP32
```

### Come funziona

Il training in mixed precision usa FP16 dove possibile (forward, backward) e mantiene FP32 dove la precisione e' critica (optimizer states, weight update):

```
Master weights (FP32) ← tenuti per l'optimizer
       │
       ▼
  Copia in FP16
       │
       ▼
  Forward pass (FP16) → piu' veloce, meno memoria
       │
       ▼
  Loss * scale_factor  ← sposta i gradienti lontano da zero per evitare underflow in FP16
       │
       ▼
  Backward (FP16) → gradienti scalati
       │
       ▼
  Gradienti / scale_factor → gradienti reali
       │
       ▼
  Optimizer step (FP32) → aggiornamento preciso
```

### Il GradScaler

FP16 rappresenta numeri in un range limitato: da `±6e-8` (minimo) a `±65504` (massimo).

I gradienti negli ultimi layer del modello sono gia' piccoli. Propagandosi all'indietro attraverso decine di layer (chain rule moltiplica gradienti per pesi), diventano sempre piu' piccoli. Se scendono sotto `6e-8` in FP16: **underflow** → diventano zero → quel peso non viene mai aggiornato.

Il **GradScaler** risolve questo amplificando i gradienti prima che vadano in underflow:

1. Moltiplica la loss per un fattore grande (es. 65536) prima del backward
2. I gradienti vengono calcolati su questa loss amplificata → sono tutti piu' grandi → niente underflow
3. Dopo il backward, divide i gradienti per lo stesso fattore → tornano ai valori reali in FP32
4. Se compaiono `inf` o `nan` (il fattore era troppo grande, overflow), salta lo step e dimezza il fattore
5. Se per N step consecutivi va tutto bene, raddoppia il fattore (usa piu' range FP16)

```
Senza scaling:
  grad = 1e-9  →  in FP16 diventa 0  →  peso mai aggiornato

Con scaling (× 65536):
  grad_scalato = 1e-9 * 65536 = 6.5e-5  →  rappresentabile in FP16
  dopo il backward: dividi per 65536  →  torni a 1e-9 in FP32 per l'optimizer step
```

```python
scaler = torch.amp.GradScaler("cuda", enabled=FP16)

# Nel loop:
with torch.amp.autocast("cuda"):   # forward in FP16
    loss = model(input).loss

scaler.scale(loss).backward()      # backward con loss scalata
scaler.unscale_(optimizer)         # riporta gradienti alla scala reale
clip_grad_norm_(...)               # clippa i gradienti reali
scaler.step(optimizer)             # step (saltato se inf/nan)
scaler.update()                    # aggiusta il fattore di scala
```

---

## 5. Il codice del notebook spiegato riga per riga

### 5.1 Setup prima del loop

```python
no_decay = ["bias", "layernorm", "layer_norm", "rmsnorm"]
optimizer_grouped_params = [
    {
        "params": [
            p for n, p in model.named_parameters()
            if not any(nd in n.lower() for nd in no_decay) and p.requires_grad
        ],
        "weight_decay": WEIGHT_DECAY,
    },
    {
        "params": [
            p for n, p in model.named_parameters()
            if any(nd in n.lower() for nd in no_decay) and p.requires_grad
        ],
        "weight_decay": 0.0,
    },
]
```
Separiamo i parametri in due gruppi: quelli che ricevono weight decay e quelli che no (bias, layer norm). `model.named_parameters()` restituisce coppie `(nome, tensore)` per ogni parametro del modello.

---

```python
optimizer = torch.optim.AdamW(
    optimizer_grouped_params,
    lr=LEARNING_RATE,
    betas=(0.9, 0.95),
)
```
**AdamW** con `beta2=0.95` invece del default `0.999`. Nel pretraining LLM si usa `0.95` perche' rende l'optimizer piu' reattivo alle variazioni recenti dei gradienti — utile quando si processano miliardi di token diversi.

- `beta1=0.9`: media mobile esponenziale dei gradienti (momento del 1° ordine)
- `beta2=0.95`: media mobile esponenziale dei gradienti al quadrato (momento del 2° ordine, stima la varianza)

---

```python
scaler = torch.amp.GradScaler("cuda", enabled=FP16 and device.type == "cuda")
```
Crea lo scaler per il mixed precision. Se `enabled=False` (su CPU o se FP16=False), tutte le chiamate a `scaler.*` diventano no-op — il codice funziona uguale senza cambiamenti.

---

```python
optimizer.zero_grad()
```
Azzeriamo i gradienti **una volta sola prima del loop**, non all'inizio di ogni step. Con gradient accumulation, azzerammo manualmente dopo ogni optimizer step — quindi qui serve solo l'azzeramento iniziale.

---

### 5.2 Inizio del loop: learning rate scheduling

```python
for global_step in range(1, MAX_STEPS + 1):
    lr = get_lr(global_step)
    for param_group in optimizer.param_groups:
        param_group['lr'] = lr
```

`get_lr()` implementa warmup lineare + cosine decay:

```python
def get_lr(step):
    # Fase warmup: da 0 a LEARNING_RATE in WARMUP_STEPS step
    if step < WARMUP_STEPS:
        return LEARNING_RATE * step / WARMUP_STEPS

    # Fase cosine decay: da LEARNING_RATE a lr_min
    progress = (step - WARMUP_STEPS) / (MAX_STEPS - WARMUP_STEPS)
    return lr_min + 0.5 * (LEARNING_RATE - lr_min) * (1 + cos(π * progress))
```

Graficamente:

```
LR
 |        /‾‾‾\
 |       /     \
 |      /       \___
 |     /              \___
 |____/                    \___  lr_min
 +----+----+----+----+----+---->  step
  warmup        cosine decay
```

`optimizer.param_groups` e' una lista di dizionari (uno per gruppo di parametri). Aggiorniamo `'lr'` in ognuno perche' avevamo due gruppi (con e senza weight decay).

---

### 5.3 Gradient accumulation

```python
accumulated_loss = 0.0

for micro_step in range(GRADIENT_ACCUMULATION_STEPS):
    batch = next(train_iter)
    input_ids = batch['input_ids'].to(device)
    labels = input_ids.clone()
```

`next(train_iter)` preleva il prossimo batch dal dataloader. `.to(device)` sposta il tensore dalla RAM CPU alla VRAM GPU — necessario prima di ogni operazione sul modello.

---

```python
    with torch.amp.autocast("cuda", enabled=FP16 and device.type == "cuda"):
        outputs = model(input_ids=input_ids, labels=labels)
        loss = outputs.loss / GRADIENT_ACCUMULATION_STEPS
```

**`autocast`**: dentro questo contesto, PyTorch usa automaticamente FP16 per le operazioni che lo supportano (matmul, convoluzioni) e mantiene FP32 per le operazioni numericamente sensibili (softmax, layer norm). Non dobbiamo fare nulla manualmente.

La divisione `/ GRADIENT_ACCUMULATION_STEPS` e' la normalizzazione della loss: stiamo accumulando gradienti di N micro-batch, quindi dobbiamo mediare.

---

```python
    scaler.scale(loss).backward()
    accumulated_loss += loss.item()
```

`scaler.scale(loss)` moltiplica la loss per il `scale_factor` interno (es. 65536) e restituisce un nuovo tensore. `.backward()` calcola i gradienti di questo tensore scalato rispetto a tutti i parametri del modello e li **somma** ai gradienti gia' nel buffer (non li sovrascrive).

`.item()` estrae il valore scalare Python dal tensore (senza fare `.item()`, terremmo un riferimento al grafo computazionale → memory leak).

---

### 5.4 Clipping e optimizer step

```python
scaler.unscale_(optimizer)
```

Divide tutti i gradienti nel buffer per lo `scale_factor`. Deve avvenire:
- **Dopo** l'ultimo backward (altrimenti i gradienti non ci sono ancora)
- **Prima** del gradient clipping (dobbiamo clippare i gradienti reali, non quelli scalati)

Il `_` finale indica un'operazione **in-place** — modifica i gradienti direttamente nel buffer.

---

```python
torch.nn.utils.clip_grad_norm_(model.parameters(), MAX_GRAD_NORM)
```

Calcola la norma L2 globale di tutti i gradienti:

```
norma = sqrt( sum( grad_i² per ogni parametro i ) )
```

Se `norma > MAX_GRAD_NORM`:
```
grad_i = grad_i * (MAX_GRAD_NORM / norma)   per ogni i
```

La direzione dei gradienti rimane uguale, cambia solo la lunghezza. Con `MAX_GRAD_NORM=1.0`, i gradienti non potranno mai avere norma superiore a 1 → niente esplosioni.

---

```python
scaler.step(optimizer)
```

Internamente fa due cose:
1. Controlla se nei gradienti ci sono `inf` o `nan` (segnale di overflow FP16)
2. Se OK: chiama `optimizer.step()` — aggiorna i pesi usando i gradienti
3. Se non OK: salta lo step silenziosamente

---

```python
scaler.update()
```

Aggiusta dinamicamente lo `scale_factor` per il prossimo step:
- Se l'ultimo step e' andato bene (niente `inf`/`nan`): dopo N step buoni consecutivi, raddoppia lo scale factor (usa piu' range FP16)
- Se lo step e' stato saltato: dimezza lo scale factor (troppo grande, causava overflow)

---

```python
optimizer.zero_grad()
```

Azzera il buffer dei gradienti. Da qui la gradient accumulation riparte da zero per il prossimo step effettivo. Se non lo chiamassimo, i gradienti del passo precedente si sommerebbero a quelli del passo corrente — disastro.

---

### 5.5 Logging e checkpoint

```python
step_losses.append(accumulated_loss)
running_loss += accumulated_loss

if global_step % LOG_EVERY == 0:
    avg_loss = running_loss / LOG_EVERY
    perplexity = math.exp(avg_loss) if avg_loss < 100 else float('inf')
```

**Perplexity** = `exp(cross_entropy_loss)`. E' la metrica standard per valutare i language model:
- Se la loss e' 0: perplexity = 1 (perfetto, impossibile)
- Se la loss e' 4.6: perplexity = 100 (il modello e' incerto tra 100 opzioni in media)
- All'inizio del training con vocab ~50k: perplexity ≈ 50000 (random)
- Un buon modello su TinyStories: perplexity < 20

```python
if global_step % SAVE_EVERY == 0:
    model.save_pretrained(checkpoint_dir)
    tokenizer.save_pretrained(checkpoint_dir)
    torch.save({
        'step': global_step,
        'optimizer_state_dict': optimizer.state_dict(),
        'scaler_state_dict': scaler.state_dict(),
        'train_losses': train_losses,
    }, os.path.join(checkpoint_dir, 'training_state.pt'))
```

Salviamo sia il modello (pesi) sia lo stato del training (optimizer, scaler, losses). Cosi' possiamo **riprendere il training** da un checkpoint senza ricominciare da zero — fondamentale per sessioni lunghe su Kaggle.

---

## 6. Schema visivo di uno step completo

```
┌─────────────────────────────────────────────────────────────────┐
│                        STEP global_step                         │
│                                                                 │
│  Aggiorna learning rate                                         │
│  lr = warmup_or_cosine(global_step)                             │
│                                                                 │
│  ┌── micro_step 0 ─────────────────────────────────────────┐   │
│  │  batch = next(train_iter)                .to(GPU)        │   │
│  │  with autocast(FP16):                                    │   │
│  │      output = model(input_ids, labels)                   │   │
│  │      loss = output.loss / N_ACCUM                        │   │
│  │  scaler.scale(loss).backward()  ← buffer += grad_0       │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ┌── micro_step 1 ─────────────────────────────────────────┐   │
│  │  ...                                                     │   │
│  │  scaler.scale(loss).backward()  ← buffer += grad_1       │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ...                                                            │
│  ┌── micro_step N-1 ───────────────────────────────────────┐   │
│  │  scaler.scale(loss).backward()  ← buffer += grad_{N-1}  │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
│  scaler.unscale_(optimizer)   ← gradienti / scale_factor        │
│  clip_grad_norm_(..., 1.0)    ← taglia se norma > 1.0           │
│  scaler.step(optimizer)       ← W = W - lr * grad (se no nan)   │
│  scaler.update()              ← aggiusta scale_factor           │
│  optimizer.zero_grad()        ← svuota buffer                   │
│                                                                 │
│  [ogni LOG_EVERY step]  stampa loss, perplexity, LR, tok/s      │
│  [ogni SAVE_EVERY step] salva checkpoint                        │
└─────────────────────────────────────────────────────────────────┘
```

### Riassunto dei concetti

| Tecnica | Problema | Soluzione | Dove nel codice |
|---|---|---|---|
| **Weight Decay** | Pesi che crescono → overfitting | Penalita' `-lr * λ * W` ad ogni step | `AdamW(weight_decay=0.1)` |
| **Gradient Clipping** | Gradienti esplosivi → training instabile | Ridimensiona se norma > soglia | `clip_grad_norm_(..., 1.0)` |
| **Gradient Accumulation** | Batch troppo grande per GPU | Somma gradienti su N micro-batch | Loop interno + `zero_grad()` ritardato |
| **Mixed Precision** | Memoria e velocita' | FP16 per calcolo, FP32 per update | `autocast` + `GradScaler` |
| **LR Scheduling** | LR fisso non ottimale | Warmup + cosine decay | `get_lr(step)` + aggiornamento manuale |
| **Perplexity** | Loss non interpretabile | `exp(loss)` = "quante scelte in media" | Logging ogni LOG_EVERY step |
