/* functions relating to OUT and RETURNING */

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

/* dummy callback that binds no data */
sb4 cbf_no_data(dvoid *ctxp, OCIBind *bindp, ub4 iter, ub4 index,
                      dvoid **bufpp, ub4 *alenpp, ub1 *piecep, dvoid **indpp) {
  *bufpp = (dvoid *) 0;
  *alenpp = 0;
  *indpp = (dvoid *) 0;
  *piecep = OCI_ONE_PIECE;

  return OCI_CONTINUE;
}

/* callback to stash the returned data */
sb4 cbf_get_data(dvoid *ctxp, OCIBind *bindp, ub4 iter, ub4 index,
		 dvoid **bufpp, ub4 **alenp, ub1 *piecep,
		 dvoid **indpp, ub2 **rcodepp) {

  /* get a pointer to store the pointer to the memory we alloc, and the error handle */
  cb_context_t* cbct = (cb_context_t*)ctxp;
  
  /* find out how many rows we are expecting */
  if (index == 0) {
    int rows = 0;
    OCIAttrGet((dvoid*)bindp, OCI_HTYPE_BIND, (dvoid*)&rows, (ub4*)sizeof(int), OCI_ATTR_ROWS_RETURNED, cbct->err);
    void* rs = (void*)malloc(rows * sizeof(out_data_t));
    memcpy(&cbct->cht.ptr, rs, sizeof(void*));
    
#ifdef DEBUG
    char dbuf[256]; snprintf(dbuf, 255, "cbf_get_data: rows=%d rs=%p cbct->cht.ptr=%p", rows, rs, cbct->cht.ptr); debug(dbuf);
#endif
  }
  
  /* storage in the order indicator, rc, bufpp, alenp - assume i can get only pointers back */
  void* offset = cbct->cht.ptr + (index * sizeof(out_data_t));
  *indpp = (dvoid*)&offset;
  *rcodepp = (dvoid*)&offset + sizeof (int);
  *bufpp = (dvoid*)&offset + (2 * sizeof(int));
  *alenp = (dvoid*)&offset + (2 * sizeof(int)) + sizeof (void*);

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "cbf_get_data: index=%d offset=%p indpp=%p rcodepp=%p bufpp=%p alenp=%p", index, offset, indpp, rcodepp, bufpp, alenp); debug(dbuf);
#endif


  *piecep = OCI_ONE_PIECE;

  return OCI_CONTINUE;
}

static struct custom_operations c_alloc_t_custom_ops = {
  "c_alloc_t_custom_ops", &caml_free_alloc_t, NULL, NULL, NULL, NULL}; 


/* allocate a bind handle as (using oci_alloc_bindhandle) ,OCIBindByPos with OCI_DATA_AT_EXEC, 
   then OCIBindDynamic with callback. Context of callback is pointer. Callback
   allocates some memory then sets the pointer to that. Assume for now that 
   OCINumber is the biggest thing

   In ML, the bind handle lives in bound_vals and the callback pointer
   lives in oci_ptrs. 
*/

/* do it for integers */
value caml_oci_bind_int_out_by_pos(value handles, value stmt, value bindh, value pos) {
  CAMLparam4(handles, stmt, bindh, pos);
  oci_handles_t h = Oci_handles_val(handles);
  OCIStmt* s = Oci_statement_val(stmt);
  OCIBind* bh = Oci_bindhandle_val(bindh);
  int p = Int_val(pos);
  sword x;
  /* allocate a pointer to a pointer */
  cb_context_t* cbct = (cb_context_t*)malloc(sizeof(cb_context_t));
  
  cbct->cht.ptr = (void*)malloc(sizeof(void*));
  cbct->err     = h.err;
  cbct->odt     = NULL;

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "caml_oci_bind_int_out_by_pos: p=%d s=%p cbct=%p cbct->cht.ptr=%p", p, s, cbct, cbct->cht.ptr); debug(dbuf);
#endif

  /* bind by position */
  int z = 0;
  x = OCIBindByPos(s, &bh, h.err, (ub4)p, (dvoid*)z, sizeof(int), SQLT_INT, 0, 0, 0, 0, 0, OCI_DATA_AT_EXEC);
  CHECK_OCI(x, h);

  /* bind in the callback */
  x = OCIBindDynamic(bh, h.err, NULL, cbf_no_data, &cbct, cbf_get_data);
  CHECK_OCI(x, h);

  value v = caml_alloc_custom(&c_alloc_t_custom_ops, sizeof(c_alloc_t), 0, 1);
  C_alloc_val(v) = cbct->cht;
  free(cbct);
  CAMLreturn(v);
}

/* end of file */
