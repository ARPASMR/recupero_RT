# recupero RT versione beta
#

library("DBI")
library("httr")
library("RMySQL")
library("jsonlite")
library("lubridate")

options(warn=0)
datainizio<-paste(today()-1," 00:00:00")
datafine<-paste(today(), " 00:00:00")
b<-seq(from=as.POSIXct(datainizio),to=as.POSIXct(datafine), "10 min")
df<-data.frame(date=b)
# virgolette dritte
options(useFancyQuotes = FALSE)
datai=dQuote(datainizio)
dataf=dQuote(datafine)
#mm contiene i valori delle date e tutti gli NA
mm<-data.frame(date=b,valore="NA")
data_inizio_recupero<-now()
#rmysql.settingsfile<-"my.cnf"
#miofile<-read.delim(rmysql.settingsfile,sep="\n")
#rmsql.user<-miofile[1,]
#rmsql.pwd<-miofile[2,]
# reinserite credenziali in chiaro per problema di dockerfile
rmsql.user<-"root"
rmsql.pwd<-"radice"
mydb = dbConnect(MySQL(), user=as.character(rmsql.user),password=as.character(rmsql.pwd), dbname='METEO', host='10.10.0.6')
myquerydati<-paste("select * from M_Osservazioni_TR where Data_e_ora between ",datai," and ", dataf)
rs = dbSendQuery(mydb, myquerydati)
mieidati=fetch(rs,-1)
vista=dbSendQuery(mydb,"select * from vw_rt10 where AggregazioneTemporale=10")
miavista=fetch(vista,-1)
# disconnessione da dB 
#dbDisconnect(mydb)
#chiedo chi sono e da dove chiamo
msg<-"hostname"
whoami<-system(msg,intern=TRUE)

#inizializzo richiesta
richiesta_header<-paste("{",dQuote("header"),": {",dQuote("id"),": 10},")
richiesta_data<-paste(dQuote("data"),":{",dQuote("sensors_list"),": [{")

# identifico elementi mancanti
# se IDoperatore=2 faccio solo il minimo, ovvero solo T
conta_update<-0
for (IDop in 1:4){
  print (paste("OPERATORE ",IDop))  
  for (i in miavista$IDsensore)  {
   # print(paste("Sensore ID ",i))
    mm<-data.frame(date=b,valore="NA")
    v<-subset(mieidati,IDsensore==i & IDoperatore==IDop ,select=c(Data_e_ora,Misura,NomeTipologia))
    v$Data_e_ora<-as.POSIXct(v$Data_e_ora)
    N<-nrow(v)
    if (N>0 & N!=145){
      #se non ho 0 oppure non ne ho 145 allora mi mancano dei dati
#      print(paste("Dati mancanti per IDsensore ", i))
     
      #mm$valore<-v$Misura[which(mm$date %in% v$Data_e_ora)]
      mm$valore[which(mm$date %in% v$Data_e_ora)]<-v$Misura
      #adesso mm contiene i valori NA solo per i dati mancanti
      y<-is.na(mm$valore)
      #grazie a y posso estrarre i valori nulli da chiedere
      hh<-mm$date[!y]
      NomeTipologia<-v$NomeTipologia[1]
      M<-NROW(hh)
      for(jj in 1:M){
        data_di_inizio<-hh[jj]
        data_di_fine.lt<-as.POSIXlt(data_di_inizio)
        data_di_fine.lt$min=data_di_fine.lt$min+1
        richiesta_vector<-paste(dQuote("sensor_id"),": ",i,",",dQuote("function_id"),": ","1",",",dQuote("operator_id"),": ",IDop,",",dQuote("granularity"),": ","1",",",dQuote("start"),": ",dQuote(data_di_inizio),",",dQuote("finish"),": ",dQuote(data_di_fine.lt),"}]}}")
        richiesta<-paste(richiesta_header,richiesta_data,richiesta_vector)
        #  print(richiesta)
        r<-POST(url="http://10.10.0.15:9090",body=noquote(richiesta))
        risposta<-fromJSON(content(r,as="text",encoding = "UTF-8")) 
       # print (paste("UPDATE...",jj," pacchetto su ", M))
        #  print(risposta)
        if (risposta$data$outcome==0 ){
          #costruisco update
          inserisci<-paste("insert into M_Osservazioni_TR (IDsensore,NomeTipologia,IDoperatore,Data_e_ora,Misura,Data,IDutente,Autore) VALUES")
          df_ins<-risposta$data$sensor_data_list
          #Misura<-strsplit(df_ins$data,";")[[2]]
          aa<-as.data.frame(strsplit(as.character(df_ins$data),";"))
          #forzo il fatto che in Misura ci deve essere un valore numerico così evito il vettore logico a lunghezza 0
          Misura<-as.double(aa[2,1])
          data_change<-format(Sys.time(), "%Y-%m-%d %H:%M:%S")
          inserisci=paste(inserisci,"(", df_ins$sensor_id,",",dQuote(NomeTipologia),",",df_ins$operator_id,",",dQuote(data_di_inizio),",",Misura,",",dQuote(data_change),",",58,dQuote(whoami),")")
          # print( inserisci)
          # send the query
          # condizione è una variabile logica che però può tornare valore a lunghezza zero 
          condizione<-!is.na(Misura) & !is.null(Misura)
          if (!is.logical(condizione)| length(condizione)==0 ){
              condizione<-FALSE #forzo condizione ad essere un valore logico
          }
          if (condizione){
            conta_update<-conta_update+1
          #  line<-readline("Mando l'Update, ok?")
            tmp <- try(dbSendQuery(mydb, inserisci), silent=TRUE)
           
               if ('try-error' %in% class(tmp)) {
                    print(tmp)
            
               }
          } 
        }
       
      } #fine del ciclo sugli M elementi mancanti
#      print(paste("...numero di dati inseriti:",conta_update))
      msg1='logger -is -p user.info "RecuperoRT: Sensore "'
      msg2= '" pacchetti" -t "RecuperoRT"'
      msg<-paste(msg1,i,conta_update,msg2)
      print(msg)
      system(msg,intern=FALSE)
      conta_update<-0
    }
    
  } #fine del ciclo sui sensori
}   # fine del ciclo sugli IDoperatore
print(paste("Recupero_RT: inizio il ",data_inizio_recupero," e fine il ", now()))
