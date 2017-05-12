#!/bin/bash
#
# lo script è all'interno del container e lancia in sequenza lo script R ogni 10 minuti
# lo script fa sempre recuperi corti ogni 20'
source ~/.bash_profile;
#  s/S = short, ovvero recupero 1h
#  l/L = long, ovvero recupero 24h
#controllo se il container è già stato lanciato: se sì, ritardo l'esecuzione 
# lo inserisco in un ciclo di attesa di 20 minuti, trascorsi i quali non eseguo il comando
SECONDS=1201
LOCKFILE='usr/local/src/myscripts/.lock'

while [ 1 ]
do
#1. verifico che lo script non sia in esecuzione
#2. verifico che siano passati 10 minuti
#2.bis verifico che il servizio di collect sia online

#3. lancio lo script
if [ $SECONDS -ge 1200 ]
then
 Rscript Recupero_RT_v0.R
 SECONDS=0
fi
done
