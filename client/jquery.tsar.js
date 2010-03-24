jQuery.tsar = function () {
	this.service = "";
	this.timeout = 5;
};

jQuery.tsar.prototype.request = function (resource, method, data, callback, settings_) {
	var url = this.service + "/" + resource;
	var settings = {
		url: url,
		data: data,
		success: callback,
		dataType: "json",
		timeout: this.timeout;
		type: method,
	};
	$.extend(true, settings, settings_);
	$.ajax(settings);
};

jQuery.tsar.prototype.get = function (resource, data, callback) {
	this.request(resource, "GET", data, callback);
};

jQuery.tsar.prototype.post = function (resource, data, callback) {
	this.request(resource, "POST", data, callback);
};
