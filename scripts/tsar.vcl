director tsar round-robin {
    { .backend = {
        .host = "144.92.181.91";
        .port = "8000";
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 5;
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
            .threshold = 5;
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
            .threshold = 5;
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
            .threshold = 5;
        }
    }}
    { .backend = {
        .host = "144.92.181.91";
        .port = "8004";
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 5;
        }
    }}
    { .backend = {
        .host = "144.92.181.91";
        .port = "8005";
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 5;
        }
    }}
    { .backend = {
        .host = "144.92.181.91";
        .port = "8006";
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 5;
        }
    }}
    { .backend = {
        .host = "144.92.181.91";
        .port = "8007";
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 5;
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
    if (req.url ~ "^/css/" &&
            req.url !~ "^/js/" &&
            req.url !~ "^/esi") {
        set obj.ttl = 15 m;
    } elseif (obj.http.Content-Type ~ "html") {
        esi;
        set obj.ttl = 1 h;
    } 
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
