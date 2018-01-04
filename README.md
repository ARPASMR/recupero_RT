# Generalità
versione beta
by MM gennaio 2017
Descrizione: lo script di R provvede al recupero dei dati da webservice e al loro inserimento nella tabella delle osservazioni del tempo reale in sinergico

Prerequisiti: *webservice funzionante, processo collect su gagliardo, librerie specifiche, dBase METEO su sinergico funzionante*

Log: chiamata diretta da R con tag _RecuperoRT_

**DockerBuild**: è stato impostato il building automatico con _dockerhub_, perciò ogni modifica al codice comporta la creazione di un immagine grazie al Dockerfile presente nel repo. La versione con tag _pgsql_ è attualmente l'unica che funziona con questa modalità e ha il Dockerfile che punta a immagini pubbliche nel repo https://hub.docker.com/u/arpasmr/

# tag pgsql
questa è la versione per inserire i dati nella tabella di IRIS Lombardia
