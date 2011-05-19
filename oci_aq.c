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

/* get the TDO of the message_type and return a pointer to it in the OCI object cache */
value caml_oci_get_tdo(value env, value handles, value type_name) {
  CAMLparam3(env, handles, type_name);
  OCIEnv* e = Oci_env_val(env);
  oci_handles_t h = Oci_handles_val(handles);
  char* t = String_val(type_name);
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_get_tdo: getting TDO for type '%s'", t); debug(dbuf);
#endif

  OCIType* tdo = (OCIType*)0;
  sword x = OCITypeByName(e, h.err, h.svc, NULL, 0, (text*)t, strlen(t), (text*)0, 0, OCI_DURATION_SESSION, OCI_TYPEGET_ALL, &tdo);
  CHECK_OCI(x, h);

  CAMLreturn((value)tdo);
}

/* end of file */
