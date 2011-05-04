# Makefile for OCI/ML project

CCFLAGS	= -ccopt -I/usr/lib/ocaml -ccopt -I$(ORACLE_HOME)/rdbms/public -ccopt -Wall
COBJS	= oci_wrapper.o 
MLOBJS	= oci_wrapper.cmi 
CCLIBS  = -cclib -L$(ORACLE_HOME)/lib -cclib -locci -cclib -lclntsh

ociml:	$(MLOBJS) $(COBJS) ociml.ml
	ocamlfind ocamlc -g -custom -o ociml $(CCLIBS) unix.cma ociml.ml $(COBJS) 

clean:
	rm *.cmi *.o *.cmo

%.cmo: %.ml
	ocamlc -g $<

%.o:	%.c
	ocamlc -g -c $(CCFLAGS) $<

%.cmi:	%.mli
	ocamlc -c $<

# End of file