 FROM r-base
COPY . /usr/local/src/myscripts
WORKDIR /usr/local/src/myscripts
CMD ["Rscript","Recupero_RT_v0.R"
