FROM mauromussin/myscriptr:recupero_RT 
COPY . /usr/local/src/myscripts
WORKDIR /usr/local/src/myscripts
CMD ["/usr/sbin/service rsyslog start"]
CMD ["Rscript","Recupero_RT_v0.R"]
