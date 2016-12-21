backend default {
    .host = "127.0.0.1";
    .port = "8889";
}

sub vcl_fetch {
    if (beresp.status == 200 && req.url ~ "swagger.json$") {
        set beresp.ttl = 604800s;  // 1 week
        return (deliver);
    }

    if (beresp.http.cache-control !~ "max-age") {
        return (hit_for_pass);
    }
}
