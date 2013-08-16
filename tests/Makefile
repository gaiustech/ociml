# Makefile for OCI*ML tests

CCLIBS  = -cclib -L$(ORACLE_HOME)/lib -cclib -lclntsh

test:	ociml_test
	./ociml_test

clean:
	rm -f ociml_test *.cm* *.o  *~ *.so *.a ocimlsh sqlnet.log *.annot

ociml_test:	testdata.cmi testdata.cmo ociml_test.ml
	ocamlfind ocamlc -g -custom  -w -8-10-26 -o ociml_test $(CCLIBS) unix.cma  -I `pwd`/.. ociml.cma  testdata.cmo ociml_test.ml

testdata.cmi:	testdata.mli
	ocamlfind ocamlc -I `pwd`/.. testdata.mli -o testdata.cmi

testdata.cmo:	testdata.ml 
	ocamlfind ocamlc -c unix.cma -w -8-10-26 -I `pwd`/.. ociml.cma testdata.ml -o testdata.cmo

# EOF