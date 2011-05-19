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

#define DEBUG

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
  
  value v = caml_alloc_custom(&c_alloc_t_custom_ops, sizeof(c_alloc_t), 0, 1);
  C_alloc_val(v) = t;

  CAMLreturn(v);
}

/* end of file */
