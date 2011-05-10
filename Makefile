# Makefile for OCI*ML project

CCFLAGS	= -ccopt -I/usr/lib/ocaml -ccopt -I$(ORACLE_HOME)/rdbms/public -ccopt -Wall
COBJS	= oci_common.o oci_connect.o oci_types.o oci_dml.o oci_select.o
MLOBJS	= log_message.cmo report.cmo ociml.cmo
CCLIBS  = -cclib -L$(ORACLE_HOME)/lib -cclib -locci -cclib -lclntsh

sample:	$(MLOBJS) $(COBJS) ociml_sample.ml
	ocamlfind ocamlc -g -custom -o ociml_sample $(CCLIBS) unix.cma $(MLOBJS) ociml_sample.ml $(COBJS) 

clean:
	rm -f ociml ociml_sample *.cmi *.o *.cmo *~ *.tgz

ociml.cmo:	ociml.ml oci_wrapper.cmi
	ocamlc -c -g ociml.ml
 
%.cmo: %.ml
	ocamlc -c -g unix.cma $<

%.o:	%.c
	ocamlc -g -c $(CCFLAGS) $<

%.cmi:	%.mli
	ocamlc -c $<

# End of file