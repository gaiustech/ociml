/* function related to parsing, binding and executing SQL */

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

static struct custom_operations oci_custom_ops = {"oci_custom_ops", NULL, NULL, NULL, NULL, NULL};

/* parse a statement for execution */
value caml_oci_stmt_prepare(value handles, value stmt, value sql) {
#ifdef DEBUG
  debug("caml_oci_stmt_prepare entered");
#endif
  CAMLparam3(handles, stmt, sql);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* sth = Oci_statement_val(stmt);
  char* sqltext = String_val(sql);
  int st_type = 0;

  sword x = OCIStmtPrepare(sth, h.err, (text*)sqltext, strlen(sqltext), OCI_NTV_SYNTAX, OCI_DEFAULT);
  CHECK_OCI(x, h)

#ifdef DEBUG
else {
    debug("caml_oci_stmt_prepare successful");
  }
#endif

 x = OCIAttrGet(sth, OCI_HTYPE_STMT, (ub2*)&st_type, 0, OCI_ATTR_STMT_TYPE, h.err);
 CHECK_OCI(x, h)
 
#ifdef DEBUG
 char dbuf[256]; snprintf(dbuf, 255, "caml_oci_stmt_prepare: stmt_type=%d", st_type); debug(dbuf);
#endif

 CAMLreturn(Val_int(st_type));
}

/* execute an already-prepared statement - throws ORA-24337 if not prepared */
value caml_oci_stmt_execute(value handles, value stmt, value autocommit, value desconly) {
#ifdef DEBUG
  debug("caml_oci_stmt_execute: entered");
#endif

  CAMLparam4(handles, stmt, autocommit, desconly);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* sth = Oci_statement_val(stmt);
  int ac = Bool_val(autocommit);
  int d = Bool_val(desconly);
  sword x;

  if (d) { /* implicit describe, but do not run query */
    x = OCIStmtExecute(h.svc, sth, h.err, 1,  0, (CONST OCISnapshot*) NULL, (OCISnapshot*) NULL, OCI_DESCRIBE_ONLY);
#ifdef DEBUG
      debug("caml_oci_stmt_execute: described only");
#endif
  } else {
    /* this may take a while */
    caml_release_runtime_system();
    if (!ac) { /* run query normally */
      x = OCIStmtExecute(h.svc, sth, h.err, 1,  0, (CONST OCISnapshot*) NULL, (OCISnapshot*) NULL, OCI_DEFAULT);
    } else { /* run query and commit immediately */
      x = OCIStmtExecute(h.svc, sth, h.err, 1,  0, (CONST OCISnapshot*) NULL, (OCISnapshot*) NULL, OCI_COMMIT_ON_SUCCESS);
#ifdef DEBUG
      debug("caml_oci_stmt_execute: autocommit is ON");
#endif
    }
    caml_acquire_runtime_system();
  }
    
  CHECK_OCI(x, h)
#ifdef DEBUG
  debug("caml_oci_stmt_execute: OK");
#endif

  CAMLreturn(Val_unit);
}


/* commit all work outstanding on this handle  */
value caml_oci_commit (value handles) {
  CAMLparam1(handles);
  oci_handles_t h = Oci_handles_val(handles);

  sword x = OCITransCommit(h.svc, h.err, 0);
  CHECK_OCI(x, h);

  CAMLreturn(Val_unit);
}

/* rollback all work outstanding on this handle  */
value caml_oci_rollback (value handles) {
  CAMLparam1(handles);
  oci_handles_t h = Oci_handles_val(handles);

  sword x = OCITransRollback(h.svc, h.err, 0);
  CHECK_OCI(x, h)

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
  CHECK_OCI(x,h);
  CAMLreturn((value)od2);  /* I am assuming the OCaml GC will just take care of this... */
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
  void* ptr;
  
  switch (dt) {
  case SQLT_STR:
    c.c = String_val(Field(colval,0));
    int l = strlen(c.c); 
    ptr = (char*)malloc(l+1);
    strncpy(ptr, c.c, l);

    x = OCIBindByPos(s, &bh, h.err, (ub4)p, (dvoid*)ptr, (sb4)l, SQLT_CHR, 0, 0, 0, 0, 0, OCI_DEFAULT);
    break;
  case SQLT_ODT: 
    debug("caml_oci_bind_by_pos: should use caml_oci_bind_date_by_pos() for dates");
    break;
  case SQLT_INT: 
    c.i = Int_val(Field(colval,0)); 
    ptr = (int*)malloc(sizeof(int)); 
    memcpy(ptr, &c.i, sizeof(int));  
    x = OCIBindByPos(s, &bh, h.err, (ub4)p, (dvoid*)ptr, (sb4) sizeof(int), SQLT_INT, 0, 0, 0, 0, 0, OCI_DEFAULT); 
    break;
  case SQLT_FLT:
    c.f = Double_val(Field(colval, 0));
    ptr = (double*)malloc(sizeof(double));
    memcpy(ptr, &c.f, sizeof(double));
    x = OCIBindByPos(s, &bh, h.err, (ub4)p, (dvoid*)ptr, (sb4) sizeof(double), SQLT_FLT, 0, 0, 0, 0, 0, OCI_DEFAULT);
    break;
  default:
    debug("oci_bind_by_pos: unexpected datatype");
  }

  CHECK_OCI(x, h);
  
  CAMLreturn((value)ptr);
}


/* same again but by name */
value caml_oci_bind_date_by_name(value handles, value stmt, value bindh, value name, value colval) {
  CAMLparam5(handles, stmt, bindh, name, colval);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* s = Oci_statement_val(stmt);
  OCIBind* bh = Oci_bindhandle_val(bindh);
  char* n = String_val(name);
  double epoch = Double_val(colval); /* datetime is epoch at this point - must convert to Oracle */

  OCIDate ocidate;
  epoch_to_ocidate(epoch, &ocidate);
  /* allocate a new OCIDate on the heap and copy onto it */
  OCIDate* od2 = (OCIDate*)malloc(sizeof(OCIDate));
  memcpy(od2, &ocidate, sizeof(OCIDate));

  sword x = OCIBindByName(s, &bh, h.err, (text*)n, (sb4)strlen((char*)n),(dvoid*)od2, (sb4)sizeof(OCIDate), SQLT_ODT, 0, 0, 0, 0, 0, OCI_DEFAULT);
  CHECK_OCI(x, h);

   CAMLreturn((value)od2);  /* I am assuming the OCaml GC will just take care of this... */
}

/* bind colval at position pos using bind handle bh in statement stmt OR to named parameter name depending on bindtype */
value caml_oci_bind_by_name(value handles, value stmt, value bindh, value posandtype, value colval) {
#ifdef DEBUG
  debug("caml_oci_bind_by_name: entered");
#endif
  CAMLparam5(handles, stmt, bindh, posandtype, colval);

  int dt = Int_val(Field(posandtype, 1));
  char* n = String_val(Field(posandtype, 0));
#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "binding datatype %d to position '%s'", dt, n); debug(dbuf);
#endif

  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* s = Oci_statement_val(stmt);
  OCIBind* bh = Oci_bindhandle_val(bindh);

  union cv_t {
    char* c;
    int i;
    double f;
  } c;
  
  sword x;
  void* ptr;
  
  switch (dt) {
  case SQLT_STR:
    c.c = String_val(Field(colval,0));
    int l = strlen(c.c); 
    ptr = (char*)malloc(l+1);
    strncpy(ptr, c.c, l);

    x = OCIBindByName(s, &bh, h.err, (text*)n, (sb4)strlen((char*)n), (dvoid*)ptr, (sb4)l, SQLT_CHR, 0, 0, 0, 0, 0, OCI_DEFAULT);
    break;
  case SQLT_ODT: 
    debug("caml_oci_bind_by_pos: should use caml_oci_bind_date_by_name() for dates");
    break;
  case SQLT_INT: 
    c.i = Int_val(Field(colval,0)); 
    ptr = (int*)malloc(sizeof(int)); 
    memcpy(ptr, &c.i, sizeof(int));  
    x = OCIBindByName(s, &bh, h.err, (text*)n, (sb4)strlen((char*)n), (dvoid*)ptr, (sb4) sizeof(int), SQLT_INT, 0, 0, 0, 0, 0, OCI_DEFAULT); 
    break;
  case SQLT_FLT:
    c.f = Double_val(Field(colval, 0));
    ptr = (double*)malloc(sizeof(double));
    memcpy(ptr, &c.f, sizeof(double));
    x = OCIBindByName(s, &bh, h.err, (text*)n, (sb4)strlen((char*)n), (dvoid*)ptr, (sb4) sizeof(double), SQLT_FLT, 0, 0, 0, 0, 0, OCI_DEFAULT);
    break;
  default:
    debug("caml_oci_bind_by_pos: unexpected datatype");
  }

  CHECK_OCI(x, h);
  
#ifdef DEBUG
  debug("bind by name OK");
#endif

  CAMLreturn((value)ptr);
}

 
/* end of file */
