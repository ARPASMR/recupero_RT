#!/bin/bash
#
# script per il lancio del container con le impostazioni di env settate
#    usa la funzione in ~./bashrc che definisce le variabili
#    per convenzione le variabili di ambiente sono minuscole
source ~/.bash_profile;
#verifico i parametri con cui è lanciato
#  s/S = short, ovvero recupero 1h
#  l/L = long, ovvero recupero 24h
#controllo se il container è già stato lanciato: se sì, ritardo l'esecuzione 
# lo inserisco in un ciclo di attesa di 20 minuti, trascorsi i quali non eseguo il comando
SECONDS=0
launched=`docker ps |grep recupero_RT| gawk '{print $1}'`
echo "$launched ${#launched}"
while [ ${#launched} -gt 0 ]
do
#sleep
    sleep 60
#   se è passato più di 19 min allora esco
    if [ $SECONDS -gt 1140 ] 
    then
        exit 1
    fi
    launched=`docker ps |grep recupero_RT| gawk '{print $1}'`
echo "$launched ${#launched}"
done
# sono uscito dal loop, quindi posso lanciare il comando
# docker run -i -e "USERDB=$USERDB" -e "USERID=$USERID" -e "USERPWD=$USERPWD" -e "DBIP=$DBIP" -v /dev/log:/dev/log mauromussin/myscriptr:recupero_RTa
echo "ora lancio docker!"

if [ $1 == "s" ]
then
   set LONG_SHORT=$1
   echo "richiesto recupero di tipo corto $LONG_SHORT"
else
   set LONG_SHORT="L"
   echo "richiesto recupero di tipo non corto $LONG_SHORT"
fi
docker run -i -e "USERDB=$USERDB" -e "USERID=$USERID" -e "USERPWD=$USERPWD" -e "DBIP=$DBIP"  -e "LONG_SHORT=$LONG_SHORT" -v /dev/log:/dev/log mauromussin/myscriptr:recupero_RT-pgsql
