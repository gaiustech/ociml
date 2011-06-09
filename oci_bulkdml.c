/* functions relating to array/bulk DML */

#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/custom.h>
#include <caml/callback.h>
#include <caml/fail.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <oci.h>
#include <ocidfn.h>
#include "oci_wrapper.h"

#if OCAML_VERSION_MINOR >= 12
#include <caml/threads.h>
#else
#include <caml/signals.h>
#endif 

/* from threads.h in 3.12 only */
#ifndef caml_acquire_runtime_system
#define caml_acquire_runtime_system caml_leave_blocking_section
#define caml_release_runtime_system caml_enter_blocking_section
#endif

/* sizes of the datatypes */
value caml_oci_get_size_of_int(value unit) { CAMLparam1(unit); CAMLreturn(Val_int(sizeof(int))); }
value caml_oci_get_size_of_float(value unit) { CAMLparam1(unit); CAMLreturn(Val_int(sizeof(double))); }

/* this is not a primitive type and will need padding to align correctly */
value caml_oci_get_size_of_date(value unit) { 
  CAMLparam1(unit); 

  int s = sizeof(OCIDate);
  int packed_size = ((s / sizeof(void*)) + 1) * (sizeof(void*));

  CAMLreturn(Val_int(packed_size));
}

/* write a native int at an offset in a heap alloc */
value caml_oci_write_nat_int_at_offset( value cht, value offset, value newint ) {
  CAMLparam3(cht, offset, newint);
  c_alloc_t t = C_alloc_val(cht);
  int o = Int_val(offset);
  int ni = Int_val(newint);
  
  memcpy(t.ptr + o, &ni, sizeof(int));

  CAMLreturn(Val_unit);
}

/* writea native double at an offset in a heap alloc */
value caml_oci_write_nat_flt_at_offset(value cht, value offset, value newdouble ) {
  CAMLparam3(cht, offset, newdouble);
  c_alloc_t t = C_alloc_val(cht);
  int o = Int_val(offset);
  double nd = Double_val(newdouble);
  
  memcpy(t.ptr + o, &nd, sizeof(double));
  CAMLreturn(Val_unit);
}

/* write a native string (null-terminated char array) at an offset within a heap alloc */
value caml_oci_write_chr_at_offset(value cht, value offset, value newstring) {
  CAMLparam3(cht, offset, newstring);
  c_alloc_t t = C_alloc_val(cht);
  int o = Int_val(offset);
  char* s = String_val(newstring);

  memcpy(t.ptr + o, s, strlen(s));

  CAMLreturn(Val_unit);
}

/* construct an OCIDate from an epoch and write it at an offset */
value caml_oci_write_odt_at_offset(value cht, value offset, value newdate) {
  CAMLparam3(cht, offset, newdate);
  c_alloc_t t = C_alloc_val(cht);
  int o = Int_val(offset);
  double epoch = Double_val(newdate);

  OCIDate od;
  epoch_to_ocidate(epoch, &od);
  memcpy(t.ptr + o, &od, sizeof(OCIDate));

  CAMLreturn(Val_unit);
}

/* bind an array of native ints as SQLT_INT */
value caml_oci_bind_bulk_int(value handles, value stmt, value bindh, value cht, value pos) {
  CAMLparam5(handles, stmt, bindh, cht, pos);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* s = Oci_statement_val(stmt);
  OCIBind* bh = Oci_bindhandle_val(bindh);
  c_alloc_t t = C_alloc_val(cht);
  int p = Int_val(pos);

  sword x = OCIBindByPos(s, &bh, h.err, (ub4)p, (dvoid*)t.ptr, (sb4) sizeof(int), SQLT_INT, NULL, NULL, NULL, 0, NULL, OCI_DEFAULT);
  CHECK_OCI(x, h);

  CAMLreturn(Val_unit);
}

/* bind an array of native doubles as SQLT_FLT (or maybe SQLT_BDOUBLE?) */
value caml_oci_bind_bulk_flt(value handles, value stmt, value bindh, value cht, value pos) {
  CAMLparam5(handles, stmt, bindh, cht, pos);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* s = Oci_statement_val(stmt);
  OCIBind* bh = Oci_bindhandle_val(bindh);
  c_alloc_t t = C_alloc_val(cht);
  int p = Int_val(pos);

  sword x = OCIBindByPos(s, &bh, h.err, (ub4)p, (dvoid*)t.ptr, (sb4) sizeof(double), SQLT_BDOUBLE, NULL, NULL, NULL, 0, NULL, OCI_DEFAULT);
  CHECK_OCI(x, h);

  CAMLreturn(Val_unit);
}

/* bind a string array as type SQLT_STR */
value caml_oci_bind_bulk_chr(value handles, value stmt, value bindh, value cht, value skipandpos) {
  CAMLparam5(handles, stmt, bindh, cht, skipandpos); 
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* s = Oci_statement_val(stmt);
  OCIBind* bh = Oci_bindhandle_val(bindh);
  c_alloc_t t = C_alloc_val(cht);

  int sk = Int_val(Field(skipandpos, 0));
  int p = Int_val(Field(skipandpos, 1));

  sword x = OCIBindByPos(s, &bh, h.err, (ub4)p, (dvoid*)t.ptr, (sb4)sk, SQLT_STR, NULL, NULL, NULL, 0, NULL, OCI_DEFAULT);
  CHECK_OCI(x, h);
  
  CAMLreturn(Val_unit);
}

/* */
value caml_oci_bind_bulk_odt(value handles, value stmt, value bindh, value cht, value skipandpos) {
  CAMLparam5(handles, stmt, bindh, cht, skipandpos); 
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* s = Oci_statement_val(stmt);
  OCIBind* bh = Oci_bindhandle_val(bindh);
  c_alloc_t t = C_alloc_val(cht);

  int sk = Int_val(Field(skipandpos, 0));
  int p = Int_val(Field(skipandpos, 1));

  sword x = OCIBindByPos(s, &bh, h.err, (ub4)p, (dvoid*)t.ptr, (sb4)sk, SQLT_ODT, NULL, NULL, NULL, 0, NULL, OCI_DEFAULT);
  CHECK_OCI(x, h);

  CAMLreturn(Val_unit);
}

value caml_oci_bulk_exec(value handles, value stmt, value num_rows, value auto_commit) {
  CAMLparam4(handles, stmt, num_rows, auto_commit);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* s = Oci_statement_val(stmt);
  int nr = Int_val(num_rows);
  int ac = Bool_val(auto_commit);
  sword x;

  caml_release_runtime_system();
  if (!ac) { /* run query normally */
    x = OCIStmtExecute(h.svc, s, h.err, nr,  0, (OCISnapshot*) NULL, (OCISnapshot*) NULL, OCI_DEFAULT);
  } else { /* run query and commit immediately */
    x = OCIStmtExecute(h.svc, s, h.err, nr,  0, (OCISnapshot*) NULL, (OCISnapshot*) NULL, OCI_COMMIT_ON_SUCCESS);
#ifdef DEBUG
    debug("caml_oci_bulk_exec: autocommit is ON");
#endif
  }
  caml_acquire_runtime_system();
  
  CAMLreturn(Val_unit);
}

/* end of file */
