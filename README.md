# Newswire
## Installazione
1. Installare docker sulla macchina --> [Docker Installation](https://docs.docker.com/install/linux/docker-ce/ubuntu/)
2. Installare MySQL 8.0 [MySQL 8.0 Installation](https://www.digitalocean.com/community/tutorials/how-to-install-the-latest-mysql-on-ubuntu-16-04)
    1. Attenzione alla modalità di autenticazione: MySQL ha recentemente cambiato la sua modalità di default
    2. Una volta installato MySQL accedere tramite `mysql -u root -p` inserendo la password scelta
    3. Creare la nuova utenza con i seguenti comandi:
      1. `CREATE USER 'asdrubale'@'localhost' IDENTIFIED WITH mysql_native_password BY 'AsdrubaleTheBest';`[Reference](https://dev.mysql.com/doc/refman/8.0/en/create-user.html)
3. Modificare il file `/etc/my.cnf` in questo modo:

        [mysqld]
        innodb_buffer_pool_size = 1G
        datadir=/local/donofrio/mysql
        socket=/var/lib/mysql/mysql.sock
        general_log_file=/var/log/mysql.log
        general_log=1
        log-error=/var/log/mysqld.log
        pid-file=/var/run/mysqld/mysqld.pid
       
4. Selezionare una directory nel sistema in cui desideriamo tenere tutti i file relativi all'applicazione (`/local/user/newswire`)
5. Per buildare l'immagine Docker in caso di modifiche al codice `docker build -t dockerhubuser/newswire .`
6. Per pushare l'immagine appena buildata su DockerHub `docker push dockerhubuser/newswire`
6. Se l'ultima immagine si trova già su DockerHub per lanciare l'applicazione basterà eseguire `start.sh nomedellimmagine`

## Utilizzo
Per ora l'applicativo ha due modalità di utilizzo crawling e classification, ma l'ottica è di prevederne solo una a breve. Il readme verrà aggiornato di conseguenza.

## Configurazione
Di seguito un esempio di configurazione di `application.properties` con le spiegazioni delle varie voci:
### Crawling
* `crawler.depth=2` - *Profondità a cui si desidera effettuare il crawling*
* `crawler.numCrawlers=32` - *Numero di crawlers paralleli*
* `crawler.storageFolder=crawler/root` - *Folder di appoggio per il crawling*
* `crawler.seeds=seeds` - *Nome del file contenente i seeds*
* `crawler.excludeList=css,js,gif,jpg,png,mp3,mp4,zip,gz,ico,pdf,xml,img` - *Non considerare URL che contengono queste parole*
* `crawler.social.excludeList=facebook,twitter,instagram,youtube,linkedin`- *Non considerare URL che contengono queste parole*
* `crawler.timetowait=1` - *Tempo di attesa fra uno snapshot e l'altro*
* `crawler.timeunit=HOURS` - *Unità di misura dell'attesa*
* `crawler.persistence.batchSize=1024` - *Dimensione del batch da salvare su Database*
* `crawler.persistence.threadCount=12` - *Numero di thread incaricati di gestire il salvataggio dei record*
### Persistenza
* `mysql.resetAll=true` - *Distrugge tutti i dati presenti prima di iniziare l'esecuzione*
* `mysql.uri=jdbc:mysql://127.0.0.1:3306/##DB_NAME##?verifyServerCertificate=false&useSSL=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC` - *URI per connettersi al Database; contiene dei placeholders*
* `mysql.uri.dbname.placeholder=##DB_NAME##` - *Placeholder per il nome del Database*
* `mysql.jdbc.driver=com.mysql.cj.jdbc.Driver`- *Driver desiderato*
* `mysql.user=user` - *Nome utente per la connessione al Database*
* `mysql.password=password` - *Password relativa all'utenza appena specificata*

### Benchmarking
* `fSnapshot=2` - *Snapshot di partenza per l'esecuzione dei benchmark; utilizzato per motivi di batching*
* `tSnapshot=50` - *Snapshot finale per l'esecuzione dei benchmark*
* `analysisSiteName=www.bestnewswebsite.it` - *Sito da analizzare*
* `data.load.resumable = false` - *Rende possibile riprendere il benchmarking*