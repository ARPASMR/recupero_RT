# recupero RT versione 1 per pgsql
# by MM
# Script R
# questa versione è studiata per fare il recupero di 1h ogni 10 min

library("DBI")
library("httr")
library("jsonlite")
library("lubridate")
library("RPostgreSQL")

rmsql.user<-Sys.getenv("USERID")
rmsql.pwd<-Sys.getenv("USERPWD")
long_or_short<-Sys.getenv("LONG_SHORT")
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
    timeout<-19
    print("Richiesto recupero corto")
    } else {
    numero_intervalli<-145
    datainizio<-strptime(orora-86400,"%F %H:%M")
    datafine<-strptime(orora,"%F %H:%M")
    timeout<-59
    print("Richiesto recupero lungo")
}
print(numero_intervalli)
options(warn=0)
b<-seq(from=as.POSIXct(datainizio),to=as.POSIXct(datafine), "10 min")
df<-data.frame(date=b)
# virgolette dritte
options(useFancyQuotes = FALSE)
datai=sQuote(datainizio)
dataf=sQuote(datafine)
#mm contiene i valori delle date e tutti gli NA
write(paste("data di inizio",datainizio,"data di fine",datafine),stdout())
mm<-data.frame(date=b,valore="NA")
data_inizio_recupero<-now()
drv<-dbDriver("PostgreSQL")
mydb = dbConnect(drv, user=as.character(rmsql.user),password=as.character(rmsql.pwd), dbname=Sys.getenv("USERDB"), host=Sys.getenv("DBIP"))
vista=dbSendQuery(mydb,"select * from dati_di_base.anagraficasensori where datafine is NULL order by frequenza")
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
   # print(paste("Sensore ID ",i))
    mm<-data.frame(date=b,valore="NA")
    v<-subset(mieidati,idsensore==i & idoperatore==IDop ,select=c(data_e_ora,misura,nometipologia))
    v$data_e_ora<-as.POSIXct(v$data_e_ora)
    N<-nrow(v)
#    print(paste("Sensore ID",i," dati previsti",numero_intervalli,"dati effettivi",N))
    if (N!=numero_intervalli){
      #se non ho 0 oppure non ne ho 7 allora mi mancano dei dati
#      print(paste("Dati mancanti per IDsensore ", i))
      #mm$valore<-v$misura[which(mm$date %in% v$data_e_ora)]
      mm$valore[which(mm$date %in% v$data_e_ora)]<-v$misura
      #adesso mm contiene i valori NA solo per i dati mancanti
      y<-is.na(mm$valore)
      #grazie a y posso estrarre i valori nulli da chiedere
      hh<-mm$date[!y]
   #  NomeTipologia<-v$nometipologia[1]
      M<-NROW(hh)
#     print(paste("inizio ciclo su M=",M))
      for(jj in 1:M){
        data_di_inizio<-hh[jj]
        data_di_fine.lt<-as.POSIXlt(data_di_inizio)
        data_di_fine.lt$min=data_di_fine.lt$min+9
       richiesta_vector<-paste(dQuote("sensor_id"),": ",i,",",dQuote("function_id"),": ",IDfunzione,",",dQuote("operator_id"),": ",IDop,",",dQuote("granularity"),": ",IDperiodo,",",dQuote("start"),": ",dQuote(data_di_inizio),",",dQuote("finish"),": ",dQuote(data_di_fine.lt),"}]}}")
        richiesta_header<-paste("{",dQuote("header"),": {",dQuote("id"),": 10},")
        richiesta_data<-paste(dQuote("data"),":{",dQuote("sensors_list"),": [{")
        richiesta<-paste(richiesta_header,richiesta_data,richiesta_vector)
        r<-POST(url="http://10.10.0.15:9090",body=noquote(richiesta))
        risposta<-fromJSON(content(r,as="text",encoding = "UTF-8")) 
        # print (paste("UPDATE...",jj," pacchetto su ", M))
        #  print(risposta)
        if (risposta$data$outcome==0 ){
          #costruisco update
          inserisci<-paste("insert into realtime.m_osservazioni_tr (idsensore,nometipologia,idoperatore,data_e_ora,misura,data,idutente,autore) VALUES")
          df_ins<-risposta$data$sensor_data_list
          aa<-as.data.frame(strsplit(as.character(df_ins$data),";"))
          Misura<-aa[2,1]
          Data_di_misura<-substr(aa[1,1],17,36)
          data_change<-format(Sys.time(), "%Y-%m-%d %H:%M:%S")
          inserisci=paste(inserisci,"(", df_ins$sensor_id,",",sQuote(NomeTipologia),",",df_ins$operator_id,",",sQuote(Data_di_misura),",",Misura,",",sQuote(data_change),",",58,",",sQuote(whoami),")")
           # print(inserisci)
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
                    print(tmp)
            
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
#    print(paste("Tempo sul giro in minuti: ",time_spent))
    if (time_spent > timeout) {
#       esito<-system('logger -is -p user.warning "RecuperoRT-pgsql: timeout" -t "RecuperoRT"',intern=FALSE) 
       stop("Troppo tempo impiegato nel recupero: esco")
    }    
  } #fine del ciclo sui IDop
}   # fine del ciclo su idsensore
msg<-paste("Recupero_RT-pgsql: inizio il ",data_inizio_recupero," e fine il ", now())
write(msg,stdout())
msg2<-paste('logger -is -p user.notice ',dQuote(msg), '-t "RecuperoRT"')
#print(msg2)
#esito<-system(msg2,intern=FALSE)
#esito<-system(paste('logger -is -p user.notice "RecuperoRT-pgsql: durata complessiva', time_spent, '" -t "RecuperoRT"'),intern=FALSE)

