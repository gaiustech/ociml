# Makefile for OCI*ML project

CCFLAGS	= -ccopt -I/usr/lib/ocaml -ccopt -I$(ORACLE_HOME)/rdbms/public -ccopt -Wall
COBJS	= oci_common.o oci_connect.o oci_types.o oci_dml.o oci_select.o
MLOBJS	= log_message.cmo report.cmo ociml.cmo
CCLIBS  = -cclib -L$(ORACLE_HOME)/lib -cclib -lclntsh

default:$(MLOBJS) $(COBJS) ociml_sample.ml
	ocamlfind ocamlc -g -custom -o ociml_sample $(CCLIBS) unix.cma $(MLOBJS) ociml_sample.ml $(COBJS) 

clean:
	rm -f ociml ociml_sample *.cm* *.o  *~ *.tgz *.so *.a ocimlsh sqlnet.log 

install: default
	ocamlmklib -verbose -o ociml -L$(ORACLE_HOME)/lib  -lclntsh -cclib -lclntsh unix.cma $(MLOBJS) $(COBJS)
	ocamlfind install ociml META ociml.cma  dllociml.so libociml.a

uninstall:
	ocamlfind remove ociml

doc: ociml.ml
	mkdir -p doc
	rm -f doc/*.html
	ocamldoc -html -d doc  ociml.ml

shell: default
	ocamlmktop -g -custom -o ocimlsh $(CCLIBS) unix.cma $(MLOBJS) $(COBJS)

dist:
	tar czvf ociml_dist.tgz *.ml *.c *.h Makefile README LICENSE META

ociml.cmo:	ociml.ml
	ocamlc -c -g ociml.ml
 
%.cmo: %.ml
	ocamlc -c -g unix.cma $<

%.o:	%.c
	ocamlc -g -c $(CCFLAGS) $<

%.cmi:	%.mli
	ocamlc -c $<

# End of file