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
  fflush(stderr);
}

/* kick an error back into OCaml-land */
void raise_caml_exception(int exception_code, char* exception_string) {
  CAMLlocal1(e);
  e = caml_alloc_tuple(2);
  Store_field(e, 0, Val_long(exception_code));
  Store_field(e, 1, caml_copy_string(exception_string));
  caml_raise_with_arg(*caml_named_value("Oci_exception"), e);
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
  CHECK_OCI(x, h);
  
  x = OCIStmtExecute(h.svc,sth, h.err, 1,  0, (CONST OCISnapshot*) NULL, (OCISnapshot*) NULL, OCI_DEFAULT);
  CHECK_OCI(x, h);
}

/* callback function to free memory, called by the OCaml GC */
void caml_free_alloc_t(value ch) {
  CAMLparam1(ch);
  c_alloc_t x = C_alloc_val(ch);
  free(x.ptr);
  CAMLreturn0;
}

/* associate callback with datatype */
static struct custom_operations c_alloc_t_custom_ops = {
  "c_alloc_t_custom_ops", &caml_free_alloc_t, NULL, NULL, NULL, NULL}; 

value caml_alloc_c_mem(value bytes) {
  CAMLparam1(bytes);
  int b = Int_val(bytes);

  c_alloc_t c = {NULL};
  c.ptr = malloc(b);

  value v = caml_alloc_custom(&c_alloc_t_custom_ops, sizeof(c_alloc_t), 0, 1);
  C_alloc_val(v) = c;
  
  CAMLreturn(v);
}

/* return the size of a pointer - happens to be 4 bytes on my dev system */
value caml_oci_size_of_pointer(value unit) {
  CAMLparam1(unit);

  CAMLreturn(Val_int(sizeof(void*)));
}

/* write a pointer at offset bytes from cht.ptr */
value caml_write_ptr_at_offset(value cht, value offset, value newpointer) {
  CAMLparam3(cht, offset, newpointer);
  c_alloc_t c = C_alloc_val(cht);
  int o = Int_val(offset);
  c_alloc_t np = C_alloc_val(newpointer);
  
  memcpy(c.ptr + o, &np.ptr, sizeof(void*));

  CAMLreturn(Val_unit);
}

/* read a pointer at offset bytes from cht.ptr */
value caml_read_ptr_at_offset(value cht, value offset) {
  CAMLparam2(cht, offset);
  c_alloc_t c = C_alloc_val(cht);
  int o = Int_val(offset);
  c_alloc_t np = {NULL};
  
  memcpy(&np.ptr, c.ptr + o, sizeof(void*));
  
  value v = caml_alloc_custom(&c_alloc_t_custom_ops, sizeof(c_alloc_t), 0, 1);
  C_alloc_val(v) = np;
  CAMLreturn(v);
}


/* end of file */
