/* function related to actually executing SQL */

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
#include "oci_wrapper.h"

#define DEBUG 1


/* commit all work outstanding on this handle  */
value caml_oci_commit (value handles) {
  CAMLparam1(handles);
  oci_handles_t h = Oci_handles_val(handles);

  sword x = OCITransCommit(h.svc, h.err, 0);
  if (x != OCI_SUCCESS) {
    oci_non_success(h);
  }

  CAMLreturn(Val_unit);
}

/* rollback all work outstanding on this handle  */
value caml_oci_rollback (value handles) {
  CAMLparam1(handles);
  oci_handles_t h = Oci_handles_val(handles);

  sword x = OCITransRollback(h.svc, h.err, 0);
  if (x != OCI_SUCCESS) {
    oci_non_success(h);
  }

  CAMLreturn(Val_unit);
}

/* allocate a blank statement handle as part of the global env */
value caml_oci_stmt_alloc(value env) {
  CAMLparam1(env);
  OCIEnv* e = Oci_env_val(env);
  OCIStmt* sth = NULL;
  
  sword x = OCIHandleAlloc((dvoid*)e, (dvoid**) &sth, OCI_HTYPE_STMT, 0, (dvoid**) 0);   
  
  value v = caml_alloc_custom(&oci_custom_ops, sizeof(OCIStmt*), 0, 1);
  Oci_statement_val(v) = sth;
  CAMLreturn(v);
}

value caml_oci_stmt_free(value stmt) {
  CAMLparam1(stmt);
  OCIStmt* s = Oci_statement_val(stmt);
  OCIHandleFree((dvoid*)s, OCI_HTYPE_STMT);
 
  CAMLreturn(Val_unit);
}

/* end of file */
