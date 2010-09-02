# To verify syntax: varnishd -C -f ./scripts/tsar.vcl

director tsar round-robin {
    { .backend = {
        .host = "144.92.181.91";
        .port = "8000";
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
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
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
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
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
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
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
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
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
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
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
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
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
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
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 5;
        }
    }}

    { .backend = {
        .host = "144.92.181.93";
        .port = "8000";
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 5;
        }
    }}
    { .backend = {
        .host = "144.92.181.93";
        .port = "8001";
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 5;
        }
    }}
    { .backend = {
        .host = "144.92.181.93";
        .port = "8002";
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 5;
        }
    }}
    { .backend = {
        .host = "144.92.181.93";
        .port = "8003";
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 5;
        }
    }}
    { .backend = {
        .host = "144.92.181.93";
        .port = "8004";
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 5;
        }
    }}
    { .backend = {
        .host = "144.92.181.93";
        .port = "8005";
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 5;
        }
    }}
    { .backend = {
        .host = "144.92.181.93";
        .port = "8006";
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 5;
        }
    }}
    { .backend = {
        .host = "144.92.181.93";
        .port = "8007";
        .connect_timeout = 120s;
        .first_byte_timeout = 120s;
        .probe = {
            .url = "/_ping";
            .interval = 5s;
            .timeout = 10s;
            .window = 8;
            .threshold = 5;
        }
    }}

}

backend tsardev {
    .host = "tsar-dev.hep.wisc.edu";
    .port = "9000";
}

sub vcl_recv { 
    if (req.http.host ~ "^tsar-dev") {
        set req.http.host = "tsar-dev.hep.wisc.edu";
        set req.backend = tsardev;
    } else {
        set req.http.host = "tsar.hep.wisc.edu";
        set req.backend = tsar;
    }
    remove req.http.X-Forwarded-For;
    set req.http.X-Forwarded-For = client.ip;
}

sub vcl_fetch {
    if (req.url ~ "^/css/" &&
            req.url !~ "^/js/") {
        set obj.ttl = 15m;
    } elseif (obj.http.Content-Type ~ "html") {
        set obj.ttl = 30m;
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
