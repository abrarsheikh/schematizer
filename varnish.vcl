backend default {
    .host = "127.0.0.1";
    .port = "8889";
}

sub vcl_fetch {
    if (beresp.http.cache-control !~ "max-age") {
        return (hit_for_pass);
    }
}
