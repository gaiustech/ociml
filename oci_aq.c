/* function related to AQ enqueue, listen and dequeue */

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

/* associate callback with datatype */
static struct custom_operations c_alloc_t_custom_ops = {
  "c_alloc_t_custom_ops", &caml_free_alloc_t, NULL, NULL, NULL, NULL}; 

/* get the TDO of the message_type and return a pointer to it in the OCI object cache */
value caml_oci_get_tdo(value env, value handles, value type_name) {
  CAMLparam3(env, handles, type_name);
  OCIEnv* e = Oci_env_val(env);
  oci_handles_t h = Oci_handles_val(handles);
  char* t = String_val(type_name);
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_get_tdo: getting TDO for type '%s'", t); debug(dbuf);
#endif

  c_alloc_t tdo = {NULL};
  sword x = OCITypeByName(e, h.err, h.svc, NULL, 0, (text*)t, strlen(t), (text*)0, 0, OCI_DURATION_SESSION, OCI_TYPEGET_ALL, (OCIType**)&tdo.ptr);
  CHECK_OCI(x, h);

  value v = caml_alloc_custom(&c_alloc_t_custom_ops, sizeof(c_alloc_t), 0, 1);
  C_alloc_val(v) = tdo;
#ifdef DEBUG
  debug("got TDO");
#endif
  CAMLreturn(v);
}

/* performs OCIStringAssignText() and returns a pointer to the memory */
value caml_oci_string_assign_text(value env, value handles, value str) {
  CAMLparam3(env, handles, str);
  OCIEnv* e = Oci_env_val(env);
  oci_handles_t h = Oci_handles_val(handles);
  char* s = String_val(str);
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_string_assign_text: '%s'", s); debug(dbuf);
#endif
  c_alloc_t t = {NULL};

  sword x = OCIStringAssignText(e, h.err, (text*)s, strlen(s), (OCIString**)&t.ptr);
  CHECK_OCI(x, h);
  
#ifdef DEBUG
  snprintf(dbuf, 255, "caml_oci_string_assign_text: pointer is %p", t.ptr); debug(dbuf);
#endif

  value v = caml_alloc_custom(&c_alloc_t_custom_ops, sizeof(c_alloc_t), 0, 1);
  C_alloc_val(v) = t;

  CAMLreturn(v);
}

value caml_oci_int_from_number(value handles, value cht, value offset) {
  CAMLparam3(handles, cht, offset);
  oci_handles_t h = Oci_handles_val(handles);
  c_alloc_t t = C_alloc_val(cht);
  int o = Int_val(offset);

  OCINumber* on = (OCINumber*)malloc(sizeof(OCINumber));
  memcpy(&on, &t.ptr + o, sizeof(OCINumber));
  int test;
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_int_from_number: entered t.ptr=%p", t.ptr); debug(dbuf);
#endif
  sword x = OCINumberToInt(h.err, on, sizeof(int), OCI_NUMBER_SIGNED, &test);
  CHECK_OCI(x, h);
#ifdef DEBUG
  snprintf(dbuf, 255, "caml_oci_int_from_number: retrieved number from payload as %d", test); debug(dbuf);
#endif
  CAMLreturn(Val_int(test));
}

value caml_oci_flt_from_number(value handles, value cht, value offset) {
  CAMLparam3(handles, cht, offset);
  oci_handles_t h = Oci_handles_val(handles);
  c_alloc_t t = C_alloc_val(cht);
  int o = Int_val(offset);

  OCINumber* on = (OCINumber*)malloc(sizeof(OCINumber));
  memcpy(&on, &t.ptr + o, sizeof(OCINumber));
  double test;
  sword x = OCINumberToReal(h.err, on, sizeof(double), &test);
  CHECK_OCI(x, h);
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_int_from_number: retrieved number from payload as %f", test); debug(dbuf);
#endif
  CAMLreturn(caml_copy_double(test));
}

value caml_oci_string_from_string(value env, value cht) {
  CAMLparam2(env, cht);
  c_alloc_t t = C_alloc_val(cht);
  OCIEnv* e = Oci_env_val(env);
  CAMLreturn(caml_copy_string((char*)OCIStringPtr(e, t.ptr)));
}

/* actually enqueue the message */
value caml_oci_aq_enqueue(value handles, value queue_name, value message_tdo, value message, value null_message) {
  CAMLparam5(handles, queue_name, message_tdo, message, null_message);
  oci_handles_t h = Oci_handles_val(handles);
  char* qn = String_val(queue_name);
  c_alloc_t mt = C_alloc_val(message_tdo);
  c_alloc_t m = C_alloc_val(message);
  c_alloc_t nm = C_alloc_val(null_message);
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_aq_enqueue: enqueueing message on '%s'",  qn); debug(dbuf);
#endif

  sword x = OCIAQEnq(h.svc, h.err, (text*)qn, 0, 0, mt.ptr, (dvoid**)&m.ptr, (dvoid**)&nm.ptr, 0, 0);
  CHECK_OCI(x, h);
#ifdef DEBUG
  debug("caml_oci_aq_enqueue: message enqueued successfully");
#endif
  CAMLreturn(Val_unit);
}

/* dequeue a message */
value caml_oci_aq_dequeue(value env, value handles, value queue_name, value message_tdo, value message_size) {
  CAMLparam5(env, handles, queue_name, message_tdo, message_size);
  //OCIEnv* e = Oci_env_val(env);
  oci_handles_t h = Oci_handles_val(handles);
  char* qn = String_val(queue_name);
  c_alloc_t mt = C_alloc_val(message_tdo);
  //int mz = Int_val(message_size);
  sword x;
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_aq_dequeue: dequeueing message from '%s'",  qn); debug(dbuf);
#endif

  void* ind_buf = NULL;
  c_alloc_t msg_buf = {NULL};
  caml_release_runtime_system();
  x = OCIAQDeq(h.svc, h.err, (text*)qn, 0, 0, mt.ptr, (dvoid**)&msg_buf.ptr, (dvoid**)&ind_buf, 0, 0);
  caml_acquire_runtime_system();
  CHECK_OCI(x, h);

#ifdef DEBUG
  snprintf(dbuf, 255, "pointer msg_buf.ptr=%p size=%d", msg_buf.ptr, mz); debug(dbuf);
  //snprintf(dbuf, 255, "Text: %s\n", OCIStringPtr(e, (deq+24))); debug(dbuf);
#endif
  
  value v = caml_alloc_custom(&c_alloc_t_custom_ops, sizeof(c_alloc_t), 0, 1);
  C_alloc_val(v) = msg_buf;
  CAMLreturn(v);
}

/* end of file */
