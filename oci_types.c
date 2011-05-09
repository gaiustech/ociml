/* functions for converting between OCI and OCaml datatypes, needed by bind and fetch */

#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/custom.h>
#include <caml/callback.h>
#include <caml/fail.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <oci.h>
#include "oci_wrapper.h"

#define DEBUG 1

/* convert an epoch time to an Oracle date as per http://www.ixora.com.au/notes/date_representation.htm */
void epoch_to_ocidate(double d, OCIDate* ocidate) {
  time_t t = (time_t)d;
  struct tm *ut = localtime(&t); /* convert to a Unix time */

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "epoch_to_ocidate: epoch=%f year=%d month=%d day=%d", d, ut->tm_year + 1900, ut->tm_mon + 1, ut->tm_mday); debug(dbuf);
#endif

  OCIDateSetDate(ocidate, ut->tm_year + 1900, ut->tm_mon + 1, ut->tm_mday); 
  OCIDateSetTime(ocidate, ut->tm_hour + 1, ut->tm_min + 1, ut->tm_sec + 1);
}

/* end of file */
