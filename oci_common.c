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
  char datebuf[32];
  time_t t = time(NULL);
  strftime((char*)&datebuf, 31, "%a %b %e %T %Y", (gmtime(&t)));
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
  int len = strlen(errbuf) - 1;
  errbuf[len] = '\0'; /* remove annoying trailing newline Oracle puts on exception messages */
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
#ifdef DEBUG
  debug("caml_free_alloc_t: entered");
#endif
  c_alloc_t x = C_alloc_val(ch);
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_free_alloc_t: x.ptr=%p x.managed_by_oci=%d", x.ptr, x.managed_by_oci); debug(dbuf);
#endif
  if (!x.managed_by_oci) {
    free(x.ptr);
  }
  CAMLreturn0;
}

/* associate callback with datatype */
static struct custom_operations c_alloc_t_custom_ops = {
  "c_alloc_t_custom_ops", &caml_free_alloc_t, NULL, NULL, NULL, NULL}; 

value caml_alloc_c_mem(value bytes) {
  CAMLparam1(bytes);
  int b = Int_val(bytes);

  c_alloc_t c = {NULL, 0};
  c.ptr = malloc(b);
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_alloc_c_mem: allocated %d bytes at address %p", b, c.ptr); debug(dbuf);
#endif

  value v = caml_alloc_custom(&c_alloc_t_custom_ops, sizeof(c_alloc_t), 0, 1);
  C_alloc_val(v) = c;
  
  CAMLreturn(v);
}

/* return the size of a pointer - happens to be 4 bytes on my dev system */
value caml_oci_size_of_pointer(value unit) {
  CAMLparam1(unit);

  CAMLreturn(Val_int(sizeof(void*)));
}

/* return the size of an OCINumber - happens to be 22 bytes on my dev system */
value caml_oci_size_of_number(value unit) {
  CAMLparam1(unit);
  int s = sizeof(OCINumber);
  int packed_size = ((s / sizeof(void*)) + 1) * (sizeof(void*));
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_size_of_number: actual size is %d, padding to %d", s, packed_size); debug(dbuf);
#endif
  CAMLreturn(Val_int(packed_size));
}

value caml_oci_write_int_at_offset(value handles, value cht, value offset, value newinteger) {
  CAMLparam4(handles, cht, offset, newinteger);
  oci_handles_t h = Oci_handles_val(handles);
  c_alloc_t c = C_alloc_val(cht);
  int o = Int_val(offset);
  int ni = Int_val(newinteger); 
  sword x;

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_write_int_at_offset: creating OCINumber for %d at offset %d from %p", ni, o, c.ptr); debug(dbuf);
#endif
  //OCINumber* on;
  //OCIMemoryAlloc(h.ses, h.err, (dvoid**)&on, OCI_DURATION_STATEMENT, sizeof(OCINumber), OCI_MEMORY_CLEARED);
  //OCINumberSetZero(h.err, on);
  OCINumber on;
#ifdef DEBUG
  debug("allocated memory");
#endif

  x = OCINumberFromInt(h.err, &ni, sizeof(int), OCI_NUMBER_SIGNED, &on);
  CHECK_OCI(x, h);

  memcpy(c.ptr + o, &on, sizeof(OCINumber));

  CAMLreturn(Val_unit);
}


value caml_oci_write_flt_at_offset(value handles, value cht, value offset, value newfloat) {
  CAMLparam4(handles, cht, offset, newfloat);
  oci_handles_t h = Oci_handles_val(handles);
  c_alloc_t c = C_alloc_val(cht);
  int o = Int_val(offset);
  double nd = Double_val(newfloat); 
  sword x;

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_write_flt_at_offset: creating OCINumber for %f at offset %d", nd, o); debug(dbuf);
#endif
  OCINumber on;

  x = OCINumberFromReal(h.err, &nd, sizeof(double), &on);
  CHECK_OCI(x, h);

  memcpy(c.ptr + o, &on, sizeof(OCINumber));

  CAMLreturn(Val_unit);
}


/* write a pointer at offset bytes from cht.ptr */
value caml_write_ptr_at_offset(value cht, value offset, value newpointer) {
  CAMLparam3(cht, offset, newpointer);
  c_alloc_t c = C_alloc_val(cht);
  int o = Int_val(offset);
  c_alloc_t np = C_alloc_val(newpointer);
  
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_write_ptr_at_offset: written %d-byte pointer to %p at offset %d from %p", sizeof(void*), np.ptr, o, c.ptr); debug(dbuf);
#endif

  memcpy(c.ptr + o, &np.ptr, sizeof(void*));
  CAMLreturn(Val_unit);
}

/* write an int at offset bytes from cht.ptr */
value c_write_int_at_offset(value cht, value offset, value intdata) {
  CAMLparam3(cht, offset, intdata);
  c_alloc_t c = C_alloc_val(cht);
  int o = Int_val(offset);
  int i = Int_val(intdata);
  int* pi = &i;
  
  memcpy(c.ptr + o, pi, sizeof(int));

  CAMLreturn(Val_unit);
}


/* read a pointer at offset bytes from cht.ptr */
value caml_read_ptr_at_offset(value cht, value offset, value oci_managed_ptr) {
  CAMLparam2(cht, offset);
  c_alloc_t c = C_alloc_val(cht);
  int o = Int_val(offset);
  int omp = Bool_val(oci_managed_ptr);
  c_alloc_t np = {NULL, omp};
  
  memcpy(&np.ptr, c.ptr + o, sizeof(void*));
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_read_ptr_at_offset: read %d-byte pointer to %p at offset %d oci_managed_ptr=%d", sizeof(void*), np.ptr, o, omp); debug(dbuf);
#endif
  
  value v = caml_alloc_custom(&c_alloc_t_custom_ops, sizeof(c_alloc_t), 0, 1);
  C_alloc_val(v) = np;
  CAMLreturn(v);
}

/* return the OCI version that OCI*ML was compiled with */
value caml_oci_version() {
  CAMLparam0();
  CAMLlocal1(ver);

  ver = caml_alloc(2, 0);
  Store_field(ver, 0, Val_int(OCI_MAJOR_VERSION));
  Store_field(ver, 1, Val_int(OCI_MINOR_VERSION));

  CAMLreturn(ver);
}

/* end of file */
