director tsar round-robin {
    { .backend = {
        .host = "144.92.181.91";
        .port = "8000";
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 3;
        }
    }}
    { .backend = {
        .host = "144.92.181.91";
        .port = "8001";
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 3;
        }
    }}
    { .backend = {
        .host = "144.92.181.91";
        .port = "8002";
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 3;
        }
    }}
    { .backend = {
        .host = "144.92.181.91";
        .port = "8003";
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 3;
        }
    }}

    { .backend = {
        .host = "144.92.181.93";
        .port = "8000";
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 3;
        }
    }}
    { .backend = {
        .host = "144.92.181.93";
        .port = "8001";
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 3;
        }
    }}
    { .backend = {
        .host = "144.92.181.93";
        .port = "8002";
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 3;
        }
    }}
    { .backend = {
        .host = "144.92.181.93";
        .port = "8003";
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 3;
        }
    }}

}

sub vcl_recv { 
    set req.http.host = "tsar.hep.wisc.edu";
    set req.backend = tsar;
    remove req.http.X-Forwarded-For;
    set req.http.X-Forwarded-For = client.ip;
}

sub vcl_fetch {
    if (obj.status >= 300) {
        pass;
    }
    if (!obj.cacheable) {
        pass;
    }
}

sub vcl_deliver {
    if (obj.hits > 0) {
        set resp.http.X-Cache = "HIT";
    } else {
        set resp.http.X-Cache = "MISS";
    }
}

#sub vcl_recv {
#    set req.http.host = "tsar.hep.wisc.edu";
#    set req.backend = tsar;
#
#    if (req.http.Cache-Control ~ "no-cache") {
#        purge_url(req.url);
#    }
#
#    if (req.request == "POST") {
#        pass;
#    }
#    lookup;
#}
#
#sub vcl_fetch {
#    if (req.request == "GET" && req.url ~ "^/records") {
#        set obj.ttl = 1m;
#        deliver;
#    }
#    if (obj.status >= 300) {
#        pass;
#    }
#    if (!obj.cacheable) {
#        pass;
#    }
#    if (obj.http.Pragma ~ "no-cache" ||
#            obj.http.Cache-Control ~ "no-cache" ||
#            obj.http.Cache-Control ~ "private") {
#        pass;
#    }
#    if (obj.http.Cache-Control ~ "max-age") {
#        unset obj.http.Set-Cookie;
#        deliver;
#    }
#    pass;
#}
