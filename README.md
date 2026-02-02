# Google Drive Reorganize Batch

Batch Java per riorganizzare file su Google Drive, spostandoli dalla cartella sorgente alla cartella destinazione con struttura `[path-relativo]/ANNO/MESE/` basata sulla data di modifica del file.

## Funzionalità

- **Spostamento file**: Sposta (non copia) i file dalla sorgente alla destinazione
- **Organizzazione per data**: Crea automaticamente la struttura `ANNO/MESE` basata sul `modifiedTime` di Google Drive
- **Estrazione data da nome file**: Supporta pattern `YYYY-MM-DD`, `YYYYMMDD` e PDF provinciali `XX-NN-ANNO-SEQ.pdf`
- **Ricorsività**: Elabora ricorsivamente tutte le sottocartelle
- **Gestione conflitti**: Rinomina automaticamente i file in caso di conflitto (es. `file.txt` → `file_1.txt`)
- **Analisi pattern**: Analizza i nomi dei file per identificare pattern ricorrenti
- **Recover**: Scarica file specifici elencati in un file di testo
- **Glacier**: Archivia in ZIP i file con data precedente a una soglia, spostandoli in una cartella di archiviazione long-term
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

# Thread concorrenti per le operazioni (opzionale, default: 10)
#operation.maxThreads=10

# Elabora sottocartelle ricorsivamente (opzionale, default: true)
#folder.source.recursive=true

# ID cartella sorgente Google Drive (OBBLIGATORIO)
folder.source.id=

# ID cartella destinazione Google Drive (OBBLIGATORIO)
folder.destination.id=

# ID cartella glacier Google Drive (obbligatorio solo con -g)
#folder.glacier.id=

# Dimensione massima di ogni ZIP glacier in MB (opzionale, default: 10)
#glacier.maxZipSizeMB=10
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
| `operation.maxThreads` | `10` | Numero di thread concorrenti |
| `folder.source.recursive` | `true` | Se elaborare ricorsivamente le sottocartelle |
| `folder.glacier.id` | - | ID della cartella Google Drive per l'archiviazione glacier (obbligatorio solo con `-g`) |
| `glacier.maxZipSizeMB` | `10` | Dimensione massima in MB di ogni archivio ZIP glacier |

### Come ottenere l'ID di una cartella Google Drive

1. Apri Google Drive nel browser
2. Naviga alla cartella desiderata
3. L'ID è la parte finale dell'URL: `https://drive.google.com/drive/folders/[FOLDER_ID]`

## Utilizzo

### Sintassi

```bash
java -Dlog4j.configurationFile=file:config/log4j.xml -jar googledrivereorganize.jar [-r|-l|-a|-rec|-g YYYY-MM] [-dry]
```

### Flag disponibili

| Flag | Descrizione |
|------|-------------|
| `-r` | **Reorganize**: Sposta i file nella struttura ANNO/MESE |
| `-l` | **List**: Elenca il contenuto della cartella sorgente |
| `-a` | **Analyze**: Analizza i pattern dei nomi file |
| `-rec` | **Recover**: Scarica i file elencati in `torecover.txt` |
| `-g YYYY-MM` | **Glacier**: Archivia in ZIP i file con data <= YYYY-MM |
| `-dry` | **Dry run**: Simula le operazioni senza modificare nulla |

### Esempi

#### Elencare il contenuto della cartella sorgente
```bash
java -Dlog4j.configurationFile=file:config/log4j.xml -jar googledrivereorganize.jar -l
```

#### Analizzare i pattern dei nomi file
```bash
java -Dlog4j.configurationFile=file:config/log4j.xml -jar googledrivereorganize.jar -a
```

#### Simulare la riorganizzazione (dry run)
```bash
java -Dlog4j.configurationFile=file:config/log4j.xml -jar googledrivereorganize.jar -r -dry
```

#### Eseguire la riorganizzazione
```bash
java -Dlog4j.configurationFile=file:config/log4j.xml -jar googledrivereorganize.jar -r
```

#### Archiviare in glacier i file fino a giugno 2024
```bash
java -Dlog4j.configurationFile=file:config/log4j.xml -jar googledrivereorganize.jar -g 2024-06
```

#### Recuperare file specifici
```bash
java -Dlog4j.configurationFile=file:config/log4j.xml -jar googledrivereorganize.jar -rec
```

## Comportamento

### Struttura di destinazione (Reorganize)

I file vengono organizzati nella seguente struttura:

```
destination/
├── 2023/
│   └── 12/
│       ├── file4.xlsx              (file dalla root della sorgente)
│       └── sottocartella/
│           └── file5.doc
└── 2024/
    ├── 01/
    │   ├── file1.txt
    │   └── file2.pdf
    ├── 02/
    │   └── sottocartella/
    │       └── file3.doc
    └── 03/
        └── file6.txt
```

### Modalità Glacier

La modalità `-g YYYY-MM` naviga ricorsivamente la cartella sorgente (struttura `YYYY/MM/...`) e per ogni cartella mese con data <= alla soglia:

1. Raccoglie tutti i file
2. Li raggruppa per chiave normalizzata (es. `wsorder-2025-01-01.log.gz` → `wsorder`)
3. Per ogni gruppo, scarica i file e li compatta in archivi ZIP (con split alla dimensione massima configurata)
4. Naming ZIP: `{groupKey}_{dataMin}-{dataMax}.zip`
5. Upload degli ZIP nella cartella glacier con struttura `YYYY/MM/[relativePath]`
6. Elimina i file originali da Drive
7. Pulisce le cartelle vuote rimaste

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
