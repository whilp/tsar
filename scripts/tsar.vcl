backend tsar01 {
    .host = "144.92.181.91";
    .port = "8080";
    .probe = {
        .url = "/_ping";
        .interval = 5s;
        .timeout = 1s;
        .window = 5;
        .threshold = 3;
    }
}

backend tsar02 {
    .host = "144.92.181.93";
    .port = "8080";
    .probe = {
        .url = "/_ping";
        .interval = 5s;
        .timeout = 1s;
        .window = 5;
        .threshold = 3;
    }
}

director tsar round-robin {
{
    .backend = tsar01;
}
{   
    .backend = tsar02;
}
}

sub vcl_recv { 
    set req.http.host = "tsar.hep.wisc.edu";
    set req.backend = tsar;
}

sub vcl_fetch {
	set obj.http.X-Tsar-Backend = backend.host;
    if (obj.status >= 300) {
        pass;
    }
    if (!obj.cacheable) {
        pass;
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
