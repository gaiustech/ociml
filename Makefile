# Makefile for OCI*ML project

ANNOT=
DEBUG=
CCFLAGS	= -ccopt -I/usr/lib/ocaml -ccopt -I$(ORACLE_HOME)/rdbms/public -ccopt -Wall $(DEBUG)
COBJS	= oci_common.o oci_connect.o oci_types.o oci_dml.o oci_select.o oci_aq.o oci_blob.o oci_out.o oci_bulkdml.o oci_dcn.o
MLOBJS	= ociml_utils.cmo log_message.cmo report.cmo ociml.cmo
MLOPTOBJS	= ociml_utils.cmx log_message.cmx report.cmx ociml.cmx
CCLIBS  = -cclib -L$(ORACLE_HOME)/lib -cclib -lclntsh

OCAML_VERSION_MAJOR = `ocamlopt -version | cut -f1 -d.`
OCAML_VERSION_MINOR = `ocamlopt -version | cut -f2 -d.`
OCAML_VERSION_POINT = `ocamlopt -version | cut -f3 -d.`

all:	ociml.cma ociml.cmxa

dev:
	make shell DEBUG="-ccopt -DDEBUG" ANNOT=-annot

sample:	all examples/ociml_sample.ml
	ocamlfind ocamlc -g -custom -o examples/ociml_sample $(CCLIBS) unix.cma $(MLOBJS) examples/ociml_sample.ml $(COBJS)

test: all
	cd tests; make test

clean:
	rm -f ociml examples/ociml_sample tests/ociml_test *.cm* *.o  *~ *.so *.a ocimlsh sqlnet.log *.annot
	cd tests; make clean

install:
	ocamlfind install ociml META ociml.a ociml.cma ociml.cmxa ociml.cmi  dllociml.so libociml.a

uninstall:
	ocamlfind remove ociml

doc: ociml.ml
	mkdir -p doc
	rm -f doc/*.html
	ocamldoc -html -d doc  ociml.ml

shell: all
	ocamlmktop -g -custom -o ocimlsh $(CCLIBS) unix.cma $(MLOBJS) $(COBJS)

ociml.cma:	$(MLOBJS) $(COBJS)
	ocamlmklib -verbose -o ociml -L$(ORACLE_HOME)/lib -lclntsh -cclib -lclntsh $(MLOBJS) $(COBJS)

ociml.cmxa:	$(MLOPTOBJS) $(COBJS)
	ocamlmklib -verbose -o ociml -L$(ORACLE_HOME)/lib -lclntsh -cclib -lclntsh $(MLOPTOBJS) $(COBJS)

ociml.cmo:	ociml.ml
	ocamlc $(ANNOT) -c -g  ociml.ml

ociml.cmx:	ociml.ml
	ocamlopt -c -g  ociml.ml

%.cmo: %.ml
	ocamlc $(ANNOT) -c -g unix.cma $<

%.cmx: %.ml
	ocamlopt -c -g unix.cmxa $<

%.o:	%.c oci_wrapper.h
	ocamlc -g -ccopt -DOCAML_VERSION_MINOR=$(OCAML_VERSION_MINOR) -c $(CCFLAGS) $<

%.cmi:	%.mli
	ocamlc $(ANNOT) -c $<

# End of file
