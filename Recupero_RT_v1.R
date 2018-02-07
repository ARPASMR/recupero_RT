# recupero RT versione 1 per pgsql
# by MM
# Script R
# questa versione è studiata per fare il recupero di 1h ogni 10 min
# seleziona anche la tipologia di recupero in funzione della variabile d'ambiente
# variabili attese:
# USERID    = utente del database postgres 
# USERPWD   = password dell'utente del database
# LONG_SHORT= se è 's' recupera una sola ora, altrimenti recupera 24 ore
# DEBUG     = se è FALSE (default) non logga i messaggi aggiuntivi, altrimenti si (dev'essere TRUE)
# TIPO      = indica la tipologia da recuperare: in questo modo ogni processo recupera una specifica tipologia
#             la sintassi è quella della clausola in di SQL
#             es 'T' oppure 'T','I' ecc.
#

library("DBI")
library("httr")
library("jsonlite")
library("lubridate")
library("RPostgreSQL")

debug=FALSE
rmsql.user<-Sys.getenv("USERID")
rmsql.pwd<-Sys.getenv("USERPWD")
long_or_short<-Sys.getenv("LONG_SHORT")
debug<-Sys.getenv("DEBUG")
Tipo<-Sys.getenv("TIPO")
write(long_or_short,stdout())
# posiziono l'inizio ai 10 minuti precedenti
adesso<-strptime(now("UTC"),"%F %H:%M")
i10<-as.integer(minute(adesso)/10)*10
differenza<-minute(adesso)-i10
orora<-strptime(adesso-differenza*60,"%F %H:%M")

if (long_or_short=="s"){
    numero_intervalli<-7
    datainizio<-strptime(orora,"%F %H:%M")
    datafine<-strptime(orora+3600,"%F %H:%M")
    timeout<-9
    write("RecuperoRT: Richiesto recupero corto",stdout())
    } else {
    numero_intervalli<-145
    datainizio<-strptime(orora-86400,"%F %H:%M")
    datafine<-strptime(orora,"%F %H:%M")
    timeout<-59
    write("RecuperoRT: Richiesto recupero lungo",stdout())
}
if(debug){
          write(paste("RecuperoRT:",numero_intervalli),stdout())
}
options(warn=0)
b<-seq(from=as.POSIXct(datainizio),to=as.POSIXct(datafine), "10 min")
df<-data.frame(date=b)
# virgolette dritte
options(useFancyQuotes = FALSE)
datai=sQuote(datainizio)
dataf=sQuote(datafine)
#mm contiene i valori delle date e tutti gli NA
write(paste("RecuperoRT: data di inizio",datainizio,"data di fine",datafine),stdout())
mm<-data.frame(date=b,valore="NA")
data_inizio_recupero<-now()
drv<-dbDriver("PostgreSQL")
mydb = dbConnect(drv, user=as.character(rmsql.user),password=as.character(rmsql.pwd), dbname=Sys.getenv("USERDB"), host=Sys.getenv("DBIP"))
QuerySensori<-paste("select * from dati_di_base.anagraficasensori where datafine is NULL and nometipologia in (",dQuote(Tipo),") order by frequenza")
vista=dbSendQuery(mydb,QuerySensori)
miavista=fetch(vista,-1)
#chiedo chi sono e da dove chiamo
msg<-"hostname"
whoami<-system(msg,intern=TRUE)

#inizializzo richiesta
richiesta_header<-paste("{",dQuote("header"),": {",dQuote("id"),": 10},")
richiesta_data<-paste(dQuote("data"),":{",dQuote("sensors_list"),": [{")

# identifico elementi mancanti
conta_update<-0
# inizio del ciclo su idsensore
for (i in miavista$idsensore){
   w<-subset(miavista,idsensore==i)
   NomeTipologia<-w$nometipologia
   Frequenza<-w$frequenza
  # se la tipologia è DV,VV è 13
  # se la tipologia è T IDoperatore è 123
  # se la tipologia è PP è 4
  # se la tipologia è RG,PA,I,UR è 1
  if (NomeTipologia=='T'){lista<-c(1,2,3)}
  if (NomeTipologia=='DV'|NomeTipologia=='VV'){lista<-c(1,3)}
  if (NomeTipologia=='PP'){lista<-c(4)}
  if (NomeTipologia=='I'|NomeTipologia=='PA'|NomeTipologia=='RG'|NomeTipologia=='UR'|NomeTipologia=='N'){lista<-c(1)}
  myquerydati<-paste("select * from realtime.m_osservazioni_tr where data_e_ora between ",datai," and ", dataf, "and idsensore=", i)
  rs = dbSendQuery(mydb, myquerydati)
  mieidati=fetch(rs,-1)
  #se non trovo dati ne prendo uno a caso per avere la struttura della tabella
  if (length(mieidati)==0){
    rs=dbSendQuery(mydb,"SELECT * from realtime.m_osservazioni_tr LIMIT 1")
    mieidati=fetch(rs,-1)
  }
  # selezione IDfunzione
   IDfunzione<-1
  # selezione granularity
  IDperiodo<-1
  if (Frequenza==30){
         IDperiodo<-2
  }
  if (Frequenza==60){
         IDperiodo<-3
  } 
  if (Frequenza<10){
         IDfunzione<-3
  }
  for (IDop in lista) {
    mm<-data.frame(date=b,valore="NA")
    v<-subset(mieidati,idsensore==i & idoperatore==IDop ,select=c(data_e_ora,misura,nometipologia))
    v$data_e_ora<-as.POSIXct(v$data_e_ora)
    N<-nrow(v)
    if (N!=numero_intervalli){
      #se non ho 0 oppure non ne ho 7 allora mi mancano dei dati
      mm$valore[which(mm$date %in% v$data_e_ora)]<-v$misura
      #adesso mm contiene i valori NA solo per i dati mancanti
      y<-is.na(mm$valore)
      #grazie a y posso estrarre i valori nulli da chiedere
      hh<-mm$date[!y]
      M<-NROW(hh)
#     ciclo sugli M valori mancanti      
      for(jj in 1:M){
        data_di_inizio<-hh[jj]
        data_di_fine.lt<-as.POSIXlt(data_di_inizio)
        data_di_fine.lt$min=data_di_fine.lt$min+9
       richiesta_vector<-paste(dQuote("sensor_id"),": ",i,",",dQuote("function_id"),": ",IDfunzione,",",dQuote("operator_id"),": ",IDop,",",dQuote("granularity"),": ",IDperiodo,",",dQuote("start"),": ",dQuote(data_di_inizio),",",dQuote("finish"),": ",dQuote(data_di_fine.lt),"}]}}")
        richiesta_header<-paste("{",dQuote("header"),": {",dQuote("id"),": 10},")
        richiesta_data<-paste(dQuote("data"),":{",dQuote("sensors_list"),": [{")
        richiesta<-paste(richiesta_header,richiesta_data,richiesta_vector)
        if (debug){
         write(paste("RecuperoRT:",richiesta), stdout())
        }
        r<-POST(url="http://10.10.0.15:9099",body=noquote(richiesta))
        risposta<-fromJSON(content(r,as="text",encoding = "UTF-8")) 
        # print (paste("UPDATE...",jj," pacchetto su ", M))
        if(debug){
         write(paste("RecuperoRT:",risposta), stdout())
        }
        if (risposta$data$outcome==0 ){
          #costruisco update
          inserisci<-paste("insert into realtime.m_osservazioni_tr (idsensore,nometipologia,idoperatore,data_e_ora,misura,data,idutente,autore) VALUES")
          df_ins<-risposta$data$sensor_data_list
          aa<-as.data.frame(strsplit(as.character(df_ins$data),";"))
          Misura<-aa[2,1]
          Data_di_misura<-substr(aa[1,1],22,41)
          data_change<-format(Sys.time(), "%Y-%m-%d %H:%M:%S")
          inserisci=paste(inserisci,"(", df_ins$sensor_id,",",sQuote(NomeTipologia),",",df_ins$operator_id,",",sQuote(Data_di_misura),",",Misura,",",sQuote(data_change),",",58,",",sQuote(whoami),")")
          if (debug){
                   write(paste("RecuperoRT:",inserisci),stdout())
          }
          # send the query
          # condizione è una variabile logica che però può tornare valore a lunghezza zero 
          condizione<-!is.na(Misura) & !is.null(Misura)
          if (!is.logical(condizione)| length(condizione)==0 ){
              condizione<-FALSE #forzo condizione ad essere un valore logico
          }
          if (condizione){
            conta_update<-conta_update+1
            tmp <- try(dbExecute(mydb, inserisci), silent=TRUE)
           
               if ('try-error' %in% class(tmp)) {
                    write(paste("RecuperoRT:",tmp),stderr())
            
               }
          } 
        }
       
      } #fine del ciclo sugli M elementi mancanti
#      print(paste("...numero di dati inseriti:",conta_update))
      if (conta_update > 0) {
#          msg1='logger -is -p user.info "RecuperoRT-pgsql: Operatore Sensore "'
#          msg2= '" pacchetti" -t "RecuperoRT"'
#          msg<-paste(msg1,IDop,i,conta_update,msg2)
#          print(msg)
#          esito<-system(msg,intern=FALSE)
      }
    } #fine ciclo su N!=numero_elementi
    conta_update<-0
# inserisco controllo per interruzione recupero se è passato troppo tempo
    time_spent<-difftime(now(),data_inizio_recupero,units="mins")
    print(paste("RecuperoRT: Acquisizione della tipologia", Tipo," in minuti: ",time_spent),stdout())
    if (time_spent > timeout) {
#    il tempo trascorso è maggiore del timeout: eseguo comunque la cancellazione dei dati prima di uscire
#       esito<-system('logger -is -p user.warning "RecuperoRT-pgsql: timeout" -t "RecuperoRT"',intern=FALSE) 
        write(paste("RecuperoRT-",sQuote(Tipo)," timeout"),stderr())
        datacancella<-strptime(orora-1296000,"%F %H:%M")
        cancella<-paste("delete from realtime.m_osservazioni_tr where data_e_ora <",sQuote(datacancella))
        tmp <- try(dbExecute(mydb, cancella), silent=TRUE)
        if ('try-error' %in% class(tmp)) {
           write(paste("RecuperoRT:",tmp),stderr())
        }
        stop("Troppo tempo impiegato nel recupero: esco",stderr())
    }    
  } #fine del ciclo sui IDop
}   # fine del ciclo su idsensore

datacancella<-strptime(orora-1296000,"%F %H:%M")
cancella<-paste("delete from realtime.m_osservazioni_tr where data_e_ora <",sQuote(datacancella))
tmp <- try(dbExecute(mydb, cancella), silent=TRUE)
if ('try-error' %in% class(tmp)) {
    write(paste("RecuperoRT:",tmp),stderr())
    }

