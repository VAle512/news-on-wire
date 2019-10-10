# Istruzioni #
PRE: Immagine di News On Wire disponibile. Nella cartella build image dare il comando: docker build -t newsonwire:latest .
Il file per docker compose permette di automatizzare completamente l'esecuzione di News On Wire e del db necessario al suo funzionamento.
Attualmente News On Wire può essere esguito in due modalità: crawler e classification.
Per cambiare la modalità di funzionamento è necessario modificare il file docker-compose.yml commentando la riga relativa alla modalità che si vuole disattivare e decommentando quella della modalità che si vuole attivare.
Ovviamente prima di avviare il classificatore si devono ottenere dei dati, perciò la sequenza consigliata è: 
avvio crawling (24 ore) -> stop -> cambio modalità nel file -> avvio classification.

>>> Nota: La modalità classification è interattiva, dopo aver avviato New On Wire in questa modalità è necessario 'entrare' nel container per selezionare l'opzione corretta, cioè: modifica al file compose -> docker-compose up -d -> docker attach <nome del container di newsonwire>


--> Per AVVIARE NEWS ON WIRE posizionarsi nella directory "compose" e dare il comando: docker-compose up
    Altre opzioni possibili: 'docker compose up -d' avvia News On Wire in background.
                             'docker compose up <nome servizio>' Avvia un servizio specifico. Utile per il debug.
--> Per ARRESTARE NEWS ON WIRE posizionarsi nella directory "compose" e dare il comando: docker-compose down
    Documentazione: https://docs.docker.com/compose/


* News *
- Aggiunta utility Dockerize che consente di controllare che mysqldb sia disponibile (https://github.com/jwilder/dockerize).


@ TO-DO @
- Separare packaging Maven da creazione container.
- Ridurre dimensioni dell'immagine (alpine linux?).
- Prevedere una signola modalità in cui la classificazione si avvia dopo un periodo di crawling specificato?
- Il file pom.xml controlla diversi aspetti del progetto. Entry point e modalità di packaging vanno revisionati anche in funzione dei punti precedenti.
