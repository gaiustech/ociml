/* common routines for the OCI binding for OCaml */

#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/custom.h>
#include <caml/callback.h>
#include <caml/fail.h>
#include <string.h>
#include <stdio.h>
#include <oci.h>
#include <time.h>
#include "oci_wrapper.h"

/* write a timestamped log message {C} for C code */
void debug(char* msg) {
  char* datebuf = (char*)malloc(32);
  time_t t = time(NULL);
  strftime(datebuf, 31, "%a %b %e %T %Y", (gmtime(&t)));
  fprintf(stderr, "%s: %s {C}\n", datebuf, msg);
}

/* kick an error back into OCaml-land */
void raise_caml_exception(int exception_code, char* exception_string) {
  CAMLlocal1(e);
  e = caml_alloc_tuple(2);
  Store_field(e, 0, Val_long(exception_code));
  Store_field(e, 1, caml_copy_string(exception_string));
  caml_raise_with_arg(*caml_named_value("Ociml_exception"), e);
}  

/* extract the error code and message from the error handle and raise an exception */
void oci_non_success(oci_handles_t h) {
  text* errbuf = (text*)malloc(256);
  sb4 errcode;
  OCIErrorGet ((dvoid*) h.err, 1, NULL, &errcode, errbuf, 255,  OCI_HTYPE_ERROR);
  raise_caml_exception((int)errcode, (char*)errbuf);
}


/* run a single simple sql statement - need OCIEnv just to alloc the handle in? */
void run_sql_simple(OCIEnv* e, oci_handles_t h, char* sql) {
  OCIStmt* sth = NULL;
  sword x;

  OCIHandleAlloc((dvoid*)e, (dvoid**) &sth, OCI_HTYPE_STMT, 0, (dvoid**) 0);  
  x = OCIStmtPrepare(sth, h.err, (text*)sql, strlen(sql), OCI_NTV_SYNTAX, OCI_DEFAULT);
  if (x != OCI_SUCCESS) {
    oci_non_success(h);
  }
  
  x = OCIStmtExecute(h.svc,sth, h.err, 1,  0, (CONST OCISnapshot*) NULL, (OCISnapshot*) NULL, OCI_DEFAULT);
  if (x != OCI_SUCCESS) {
    oci_non_success(h);
  }
}


/* end of file */
