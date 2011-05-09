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
#include <ocidfn.h>
#include "oci_wrapper.h"

#define DEBUG 1

/* parse a statement for execution */
value caml_oci_stmt_prepare(value handles, value stmt, value sql) {
#ifdef DEBUG
  debug("caml_oci_stmt_prepare entered");
#endif
  CAMLparam3(handles, stmt, sql);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* sth = Oci_statement_val(stmt);
  char* sqltext = String_val(sql);

  sword x = OCIStmtPrepare(sth, h.err, (text*)sqltext, strlen(sqltext), OCI_NTV_SYNTAX, OCI_DEFAULT);
  if (x != OCI_SUCCESS) {
    oci_non_success(h);
  } 
#ifdef DEBUG
else {
    debug("caml_oci_stmt_prepare successful");
  }
#endif
  CAMLreturn(Val_unit);
}

/* execute an already-prepared statement - throws ORA-24337 if not prepared */
value caml_oci_stmt_execute(value handles, value stmt) {
  CAMLparam2(handles, stmt);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* sth = Oci_statement_val(stmt);

  sword x = OCIStmtExecute(h.svc, sth, h.err, 1,  0, (CONST OCISnapshot*) NULL, (OCISnapshot*) NULL, OCI_DEFAULT);
  if (x != OCI_SUCCESS) {
    oci_non_success(h);
  }

  CAMLreturn(Val_unit);
}


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

/* allocate a bind handle (pointer) */
value caml_oci_alloc_bindhandle(value unit) {
#ifdef DEBUG
  debug("caml_oci_alloc_bindhandle: entered");
#endif
  CAMLparam0();
  OCIBind * bh = (OCIBind*)0;
  value v = caml_alloc_custom(&oci_custom_ops, sizeof(OCIBind*), 0, 1);
  Oci_bindhandle_val(v) = bh;
  CAMLreturn(v);
}

/* allocate a blank statement handle as part of the global env */
value caml_oci_stmt_alloc(value env) {
  CAMLparam1(env);
  OCIEnv* e = Oci_env_val(env);
  OCIStmt* sth = NULL;
  
  sword x = OCIHandleAlloc((dvoid*)e, (dvoid**) &sth, OCI_HTYPE_STMT, 0, (dvoid**) 0);   
  if (x != OCI_SUCCESS) {
    raise_caml_exception(-1, "Cannot alloc handle in global env");
  }

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

/* dates are special as we need to maintain the allocated OCIDate object */
value caml_oci_bind_date_by_pos(value handles, value stmt, value bindh, value pos, value colval) {
  CAMLparam5(handles, stmt, bindh, pos, colval);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* s = Oci_statement_val(stmt);
  OCIBind* bh = Oci_bindhandle_val(bindh);
  int p = Int_val(pos);
  double epoch = Double_val(colval); /* datetime is epoch at this point - must convert to Oracle */

  OCIDate ocidate;
  epoch_to_ocidate(epoch, &ocidate);
  /* allocate a new OCIDate on the heap and copy onto it */
  OCIDate* od2 = (OCIDate*)malloc(sizeof(OCIDate));
  memcpy(od2, &ocidate, sizeof(OCIDate));
#ifdef DEBUG
  char* fmt = "DD-MON-YYYY HH24:MI:SS";
  char dbuf[256];
  ub4 dbufsize=255;
  debug("testing converted OCIDate:");
  OCIDateToText(h.err, (const OCIDate*)od2, (text*)fmt, (ub1)strlen(fmt), (text*)0, (ub4)0, &dbufsize, (text*)dbuf);
  debug(dbuf);
#endif
  sword x = OCIBindByPos(s, &bh, h.err, (ub4)p, (dvoid*)od2, (sb4)sizeof(OCIDate), SQLT_ODT, 0, 0, 0, 0, 0, OCI_DEFAULT);
   if (x != OCI_SUCCESS) {
    oci_non_success(h);
  }
   return (value)od2;  /* I am assuming the OCaml GC will just take care of this... */
}

/* bind colval at position pos using bind handle bh in statement stmt OR to named parameter name depending on bindtype */
value caml_oci_bind_by_pos(value handles, value stmt, value bindh, value posandtype, value colval) {
#ifdef DEBUG
  debug("caml_oci_bind_by_pos: entered");
#endif
  CAMLparam5(handles, stmt, bindh, posandtype, colval);

  int dt = Int_val(Field(posandtype, 1));
  int p = Int_val(Field(posandtype, 0));
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "binding datatype %d to position %d", dt, p); debug(dbuf);
#endif

  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* s = Oci_statement_val(stmt);
  OCIBind* bh = Oci_bindhandle_val(bindh);

  /* annoyingly can't create a local var immediately after a case in GCC - http://gcc.gnu.org/bugzilla/show_bug.cgi?id=37231  */
  union cv_t {
    char* c;
    int i;
    double f;
  } c;

  sword x;

  switch (dt) {
  case SQLT_STR:
    c.c = String_val(Field(colval,0));
    x = OCIBindByPos(s, &bh, h.err, (ub4)p, (dvoid*)c.c, (sb4) strlen(c.c), SQLT_CHR, 0, 0, 0, 0, 0, OCI_DEFAULT);
    break;
  case SQLT_ODT: 
    debug("caml_oci_bind_by_pos: should use oci_bind_date_by_pos() for dates");
    break;
  case SQLT_INT:
    break;
  case SQLT_FLT:
    break;
  default:
    debug("oci_bind_by_pos: unexpected datatype");
  }

  if (x != OCI_SUCCESS) {
    oci_non_success(h);
  }
  
  CAMLreturn(Val_unit);
}

 
/* end of file */
