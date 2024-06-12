# Progetto di Caricamento Dati da Azure Data Lake a PostgreSQL

Questo progetto è progettato per connettersi ad un Azure Data Lake, prelevare file CSV e caricarli in massa in un database PostgreSQL. Il progetto è suddiviso in due cartelle principali: una contenente il codice in C# e l'altra il codice in Python.

## Struttura del Progetto

```
.
├── /PostgreCopy_C#
│   ├── Program.cs
│   ├── ... (altri file C#)
│
├── /PostgresCopy-python
│   ├── main.py
│   ├── ... (altri file Python)
│
├── README.md
└── ...
```

## Funzionalità

- Connessione ad Azure Data Lake
- Estrazione di file CSV
- Caricamento in massa dei dati estratti in un database PostgreSQL

## Requisiti

- .NET Core SDK
- Python 3.x
- Moduli Python: `azure-datalake-store`, `psycopg2`, `pandas`
- Un database PostgreSQL configurato
- Credenziali di accesso ad Azure Data Lake e PostgreSQL

## Licenza

Questo progetto è sotto licenza MIT. Vedi il file [LICENSE](LICENSE) per maggiori dettagli.
