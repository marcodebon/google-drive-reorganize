# Google Drive Reorganize Batch

Batch Java per riorganizzare file su Google Drive, spostandoli dalla cartella sorgente alla cartella destinazione con struttura `[path-relativo]/ANNO/MESE/` basata sulla data di modifica del file.

## Funzionalità

- **Spostamento file**: Sposta (non copia) i file dalla sorgente alla destinazione
- **Organizzazione per data**: Crea automaticamente la struttura `ANNO/MESE` basata sul `modifiedTime` di Google Drive
- **Ricorsività**: Elabora ricorsivamente tutte le sottocartelle
- **Gestione conflitti**: Rinomina automaticamente i file in caso di conflitto (es. `file.txt` → `file_1.txt`)
- **Dry run**: Modalità simulazione per verificare le operazioni senza modifiche effettive
- **Retry automatico**: Riprova le operazioni fallite con intervallo configurabile

## Requisiti

- Java 11 o superiore
- Service Account Google con accesso a Google Drive API
- File JSON delle credenziali del Service Account

## Configurazione

### File di configurazione

Il file `config/googledrivereorganize.properties` contiene le configurazioni:

```properties
# Credenziali Service Account (opzionale, default: config/upload-gdrive-443816-e667cf3f212b.json)
#serviceAccountKeyFile=config/upload-gdrive-443816-e667cf3f212b.json

# Numero di tentativi per operazione (opzionale, default: 3)
#operation.retry=3

# Secondi tra i tentativi (opzionale, default: 10)
#operation.sleepRetry=10

# Elabora sottocartelle ricorsivamente (opzionale, default: true)
#folder.source.recursive=true

# ID cartella sorgente Google Drive (OBBLIGATORIO)
folder.source.id=

# ID cartella destinazione Google Drive (OBBLIGATORIO)
folder.destination.id=
```

### Parametri obbligatori

| Parametro | Descrizione |
|-----------|-------------|
| `folder.source.id` | ID della cartella Google Drive da cui leggere i file |
| `folder.destination.id` | ID della cartella Google Drive in cui spostare i file |

### Parametri opzionali

| Parametro | Default | Descrizione |
|-----------|---------|-------------|
| `serviceAccountKeyFile` | `config/upload-gdrive-443816-e667cf3f212b.json` | Path al file JSON delle credenziali |
| `operation.retry` | `3` | Numero di tentativi per ogni operazione |
| `operation.sleepRetry` | `10` | Secondi di attesa tra i tentativi |
| `folder.source.recursive` | `true` | Se elaborare ricorsivamente le sottocartelle |

### Come ottenere l'ID di una cartella Google Drive

1. Apri Google Drive nel browser
2. Naviga alla cartella desiderata
3. L'ID è la parte finale dell'URL: `https://drive.google.com/drive/folders/[FOLDER_ID]`

## Utilizzo

### Sintassi

```bash
java -Dlog4j.configurationFile=file:config/log4j.xml -jar googledrivereorganize.jar [-r|-l] [-dry]
```

### Flag disponibili

| Flag | Descrizione |
|------|-------------|
| `-r` | **Reorganize**: Sposta i file nella struttura ANNO/MESE |
| `-l` | **List**: Elenca il contenuto della cartella sorgente |
| `-dry` | **Dry run**: Simula le operazioni senza modificare nulla |

### Esempi

#### Elencare il contenuto della cartella sorgente
```bash
java -Dlog4j.configurationFile=file:config/log4j.xml -jar googledrivereorganize.jar -l
```

#### Simulare la riorganizzazione (dry run)
```bash
java -Dlog4j.configurationFile=file:config/log4j.xml -jar googledrivereorganize.jar -r -dry
```

#### Eseguire la riorganizzazione
```bash
java -Dlog4j.configurationFile=file:config/log4j.xml -jar googledrivereorganize.jar -r
```

## Comportamento

### Struttura di destinazione

I file vengono organizzati nella seguente struttura:

```
destination/
├── [sottocartella-sorgente]/
│   ├── 2024/
│   │   ├── 01/
│   │   │   ├── file1.txt
│   │   │   └── file2.pdf
│   │   └── 02/
│   │       └── file3.doc
│   └── 2023/
│       └── 12/
│           └── file4.xlsx
└── 2024/
    └── 03/
        └── file5.txt  (file dalla root della sorgente)
```

### Gestione dei conflitti

Se nella destinazione esiste già un file con lo stesso nome, viene aggiunto un suffisso numerico:

- `documento.pdf` → `documento_1.pdf`
- `documento_1.pdf` → `documento_2.pdf`

### Protezione da ricorsione infinita

Se `folder.source.id` = `folder.destination.id`, il batch salta automaticamente le cartelle con nomi che corrispondono a:
- **Anno**: 4 cifre (1900-2100)
- **Mese**: 2 cifre (01-12)

Questo previene loop infiniti quando sorgente e destinazione coincidono.

## Build

```bash
cd it.anitia.batch.googledrive.reorganize
mvn clean package
```

Il JAR viene generato in `../installdir/googledrivereorganize.jar`

## Log

I log vengono scritti in:
- Console (STDOUT)
- File: `logs/googledrivereorganize.log`

La configurazione del logging è in `config/log4j.xml`.

## Struttura del progetto

```
it.anitia.batch.googledrive.reorganize/
├── pom.xml
├── README.md
└── src/
    └── main/
        ├── java/
        │   └── it/anitia/batch/googledrive/reorganize/
        │       ├── App.java
        │       └── Settings.java
        └── resources/
            └── config/
                ├── googledrivereorganize.properties
                ├── log4j.xml
                └── upload-gdrive-443816-e667cf3f212b.json
```
