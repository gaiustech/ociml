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

/* convert an epoch time to an Oracle date */
void epoch_to_ocidate(double e, OCIDate* ocidate) {
  time_t t = (time_t)e;
  struct tm* ut = localtime(&t); /* convert to a Unix time */

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "epoch_to_ocidate: epoch=%f year=%d month=%d day=%d", e, ut->tm_year + 1900, ut->tm_mon + 1, ut->tm_mday); debug(dbuf);
#endif

  OCIDateSetDate(ocidate, ut->tm_year + 1900, ut->tm_mon + 1, ut->tm_mday); 
  OCIDateSetTime(ocidate, ut->tm_hour + 1, ut->tm_min, ut->tm_sec + 1);
}

/* convert an Oracle date to epoch */
double ocidate_to_epoch(OCIDate* ocidate) {
  int year, month, day, hour, minute, second;
  time_t t;
  struct tm* ut;

  OCIDateGetDate(ocidate, &year, &month, &day);
  OCIDateGetTime(ocidate, &hour, &minute, &second);

  time (&t);
  ut = localtime(&t);

  ut->tm_year = year - 1900; ut->tm_mon = month - 1; ut->tm_mday = day;
  ut->tm_hour = hour - 1;    ut->tm_min = minute;    ut->tm_sec = second - 1;

#ifdef DEBUG
  char dbuf[256]; snprintf(dbuf, 255, "ocidate_to_epoch: epoch=%f year=%d month=%d day=%d", (double)mktime(ut), ut->tm_year + 1900, ut->tm_mon + 1, ut->tm_mday); debug(dbuf);
#endif
  return (double)mktime(ut);
}

/* end of file */
