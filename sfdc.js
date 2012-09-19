/*
** Author: Philipp Dunkel <p.dunkel@durchblicker.at>
** Author: Petra Ollram <p.ollram@durchblicker.at>
** © 2012 by YOUSURE Tarifvergleich GmbH
**
** Permission is hereby granted, free of charge, to any person obtaining a copy
** of this software and associated documentation files (the "Software"), to deal
** in the Software without restriction, including without limitation the rights
** to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
** copies of the Software, and to permit persons to whom the Software is
** furnished to do so, subject to the following conditions:
**
** The above copyright notice and this permission notice shall be included in
** all copies or substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
** THE SOFTWARE.
**
*/

module.exports = Salesforce;

var https = require('https');
var util = require('util');
var querystring = require('querystring').stringify;
var parseURL = require('url').parse;
var fs = require('fs');
var extension = require('path').extname;

function login(host, username, password, credential, client, secret, callback) {
  var options = {
    host: String(host),
    port: 443,
    path: '/services/oauth2/token?' + querystring({
      'grant_type': 'password',
      'client_id': client,
      'client_secret': secret,
      'username': username,
      'password': password+credential,
      'format': 'json'
    }),
    method: 'POST',
    headers: {
      'Content-Length': 0
    }
  };


  https.request(options, function(res) {
    if (res.statusCode > 299) return callback(new Error('Invalid Login: ' + res.statusCode));
    var data = [];
    res.on('data', data.push.bind(data));
    res.on('end', function() {
      data = Buffer.concat(data);
      try {
        data = JSON.parse(data.toString('utf-8'));
      } catch (ex) {
        return callback(ex);
      }
      data.instanceHost = parseURL(data.instance_url).hostname;
      return callback(undefined, data);
    });
  }).on('error', callback).end();
}
function request(token, host, path, method, data, stream, callback) {
  var options = {
    'host': host, 
    'port': 443, 
    'path': path, 
    'method': method, 
    'headers':{ 
      'Authorization':'OAuth '+token,
      'Content-Type':'application/json'
    }
  };
  if (data) {
    options.headers['Content-Length'] = data.length;
    options.headers['Expect'] = '100-continue';
  }
  https.request(options, function(res) {
    var err;
    switch (res.statusCode) {
      case 401:
        err = new Error('Not Authenticated');
        err.clearToken = true;
        err.retry = true;
        break;
      case 403:
        err = new Error('Access Denied');
        err.clearToken = true;
        break;
      case 204:
        data = [ new Buffer(JSON.stringify({ errors:[], success:true })) ];
        break;
      default:
        if (res.statusCode > 299) {
          err = new Error('Bad Status: ' + res.statusCode);
        }
    }
    if (stream) {
      res.pause();
      return callback(err, res);
    }
    var data = [];
    res.on('data', data.push.bind(data));
    res.on('end', function() {
      data = Buffer.concat(data);
     
      if (res.headers['content-type'] && res.headers['content-type'].substr(0, 'application/json'.length) === 'application/json') {
        try {
          data = JSON.parse(data.toString('utf-8'));
        } catch (ex) {
          err = ex;
          data = undefined;
        }
      } else {
        data = { 'type': res.headers['content-type'], 'content': data };
      }
      callback(err, data);
    });
  }).on('error', callback).end(data);
}
function makeURL(ctx, command, params) {
  command = util.isArray(command) ? command : [command];
  command = [ctx.serviceURL].concat(command);
  command = [command.join('/')];
  command.push(querystring(params || {}));
  return command.join('?');
}
function execute(ctx) {

  if(!ctx.actions.length){
    return;
  }

  if (ctx.processing) return;
  if (!ctx.login) {
    ctx.processing = true;

    return login(ctx.options.loginHost || 'login.salesforce.com', ctx.options.username, ctx.options.password, ctx.options.credential, ctx.options.clientId, ctx.options.clientSecret, function(err, data) {
      ctx.processing = false;
      if (err || !data) {
        ctx.error = err || new Error('No Login');
        while (ctx.actions.length) ctx.actions.shift().callback(err);
        return;
      }
      ctx.login = data;
      execute(ctx);
    });
  }

  var action = ctx.actions.shift();
  request(ctx.login.access_token, ctx.login.instanceHost, action.command, action.method, action.data, action.stream, function(err, res) {
    if (err) {
      if (err.clearToken) ctx.login = undefined;
      if (err.retry) {
        ctx.actions.unshift(action);
      } else {
        action.callback(err, res);
      }
    } else {
      action.callback(undefined, res);
    }
    execute(ctx);
  });
}

function Salesforce(options) {
  var ctx = {
    'options': options || {},
    'actions': [],
    'serviceURL': '/services/data/v25.0'
  };

  Object.defineProperty(this, 'describe', { 'value': describe.bind(ctx, ctx) });
  Object.defineProperty(this, 'queryObjects', { 'value': query.bind(ctx, ctx) });
  Object.defineProperty(this, 'searchObjects', { 'value': search.bind(ctx, ctx) });
  Object.defineProperty(this, 'createObject', { 'value': create.bind(ctx, ctx) });
  Object.defineProperty(this, 'fetchObject', { 'value': fetch.bind(ctx, ctx) });
  Object.defineProperty(this, 'updateObject', { 'value': update.bind(ctx, ctx) });
  Object.defineProperty(this, 'upsertObject', { 'value': upsert.bind(ctx, ctx) });
  Object.defineProperty(this, 'deleteObject', { 'value': remove.bind(ctx, ctx) });
  Object.defineProperty(this, 'fetchExternalObject', { 'value': externalFetch.bind(ctx, ctx) });
  Object.defineProperty(this, 'updateExternalObject', { 'value': externalUpdate.bind(ctx, ctx) });
  Object.defineProperty(this, 'upsertExternalObject', { 'value': upsert.bind(ctx, ctx) });
  Object.defineProperty(this, 'deleteExternalObject', { 'value': externalRemove.bind(ctx, ctx) });
  Object.defineProperty(this, 'createAttachment', { 'value': attachmentCreate.bind(ctx, ctx) });
  Object.defineProperty(this, 'attachStream', { 'value': attachStream.bind(ctx, ctx) });
  Object.defineProperty(this, 'attachFile', { 'value': attachFile.bind(ctx, ctx) });
  Object.defineProperty(this, 'attachBuffer', { 'value': attachmentCreate.bind(ctx, ctx) });
  Object.defineProperty(this, 'blobStream', { 'value': blobStream.bind(ctx, ctx) });
  Object.defineProperty(this, 'attachmentStream', { 'value': attachmentStream.bind(ctx, ctx) });
  return this;
}

function describe(ctx, objectClass, callback) {
  if (!callback && ('function' === typeof objectClass)) {
    callback = objectClass;
    objectClass = ['sobjects'];
  } else {
    objectClass = ['sobjects',objectClass,'describe'];
  }
  ctx.actions.push({
    'command':makeURL(ctx, objectClass),
    'method':'GET',
    'callback':callback
  });
  execute(ctx);
}
function query(ctx, query, callback) {
  
  ctx.actions.push({
    'command':makeURL(ctx,'query',{'q':query}),
    'method':'GET',
    'callback':callback
  });
  execute(ctx);
}
function search(ctx, query, callback) {
  ctx.actions.push({
    'command':makeURL(ctx,'search',{'q':query}),
    'method':'GET',
    'callback':callback
  });
  execute(ctx);
}
function create(ctx, objectClass, object, callback) {
  ctx.actions.push({
    'command':makeURL(ctx,['sobjects',objectClass]),
    'method':'POST',
    'data':new Buffer(JSON.stringify(object)),
    'callback':callback
  });
  execute(ctx);
}
function fetch(ctx, objectClass, id, fields, callback) {
  if ((!callback) && ('function' === typeof fields)) {
    callback = fields;
    fields = undefined;
  }
  fields = ('object' === typeof fields) ? fields : undefined;
  fields = (fields && !util.isArray(fields)) ? Object.keys(fields) : fields;
  fields = (fields && fields.length) ? { 'fields':fields.join(', ') } : undefined;

  ctx.actions.push({
    'command':makeURL(ctx, ['sobjects',objectClass,id],fields),
    'method':'GET',
    'callback':callback
  });
  execute(ctx);
}
function update(ctx, objectClass, id, data, callback) {
  ctx.actions.push({
    'command':makeURL(ctx,['sobjects',objectClass,id]),
    'method':'PATCH',
    'data':new Buffer(JSON.stringify(data)),
    'callback':callback
  });
  execute(ctx);
}
function upsert(ctx, objectClass, data, indexField, callback) {
  ctx.actions.push({
    'command':makeURL(ctx,['sobjects',objectClass,indexField,data[indexField]]),
    'method':'PATCH',
    'data':new Buffer(JSON.stringify(data)),
    'callback':callback
  });
  execute(ctx);
}
function remove(ctx, objectClass, id, callback) {
  ctx.actions.push({
    'command':makeURL(ctx,['sobjects',objectClass,id]),
    'method':'DELETE',
    'callback':callback
  });
  execute(ctx);
}

function externalFetch(ctx, objectClass, indexField, indexValue, callback) {
  ctx.actions.push({
    'command':makeURL(ctx,['sobjects',objectClass,indexField,indexValue]),
    'method':'GET',
    'callback':callback
  });
  execute(ctx);
}
function externalUpdate(ctx, objectClass, data, indexField, callback) {
  ctx.actions.push({
    'command':makeURL(ctx,['sobjects',objectClass,indexField,data[indexField]]),
    'method':'PATCH',
    'data':new Buffer(JSON.stringify(data)),
    'callback':callback
  });
  execute(ctx);
}
function externalRemove(ctx, objectClass, indexField, indexValue, callback) {
  ctx.actions.push({
    'command':makeURL(ctx,['sobjects',objectClass,indexField,indexValue]),
    'method':'DELETE',
    'callback':callback
  });
  execute(ctx);
}

function attachmentCreate(ctx, parentId, name, content, type, callback) {
  content = Buffer.isBuffer(content) ? content : new Buffer(JSON.stringify(content));
  content = new Buffer(JSON.stringify({
    'ParentId':parentId, 
    'ContentType':type, 
    'Name':name, 
    'Body':content.toString('base64')
  }));
  ctx.actions.push({
    'command':makeURL(ctx,['sobjects','Attachment']),
    'method':'POST',
    'data':content,
    'callback':callback
  });
  execute(ctx);
}
function attachStream(ctx, parentId, name, stream, type) {
  var content = [];
  stream.on('data', content.push.bind(content));
  stream.on('end', function() {
    content = Buffer.concat(content);
    attachmentCreate(ctx, parentId, name, content, type, callback);
  });
  stream.on('error', callback);
}
function attachFile(ctx, parentId, filename, type, callback) {
  if (!callback && ('function' === typeof type)) {
    callback = type;
    type = undefined;
  }
  type = type || 'application/octet-stream';
  attachStream(ctx, parentId, name, fs.createReadStream(filename), type, type);
}

function blobStream(ctx, objectClass, id, fields, callback) {
  ctx.actions.push({
    'command':makeURL(ctx, ['sobjects',objectClass,id],fields),
    'method':'GET',
    'stream':true,
    'callback':callback
  });
  execute(ctx);
}

function simpleMime(name) {
  switch(extname(name || '').toLowerCase()) {
    case '.pdf' : return 'application/pdf';
    case '.jpg' : return 'image/jpeg';
    case '.jpeg' : return 'image/jpeg';
    case '.gif' : return 'image/gif';
    case '.png' : return 'image/png';
    default : return 'application/octet-stream';
  }
}
function attachmentStream(ctx, id, callback) {
  fetch(ctx, 'Attachment', id, [ 'Name', 'ContentType', 'BodyLength' ], function(err, info) {
    if (err || !info) return callback(err || new Error('No Attachment Information'));
    blobStream(ctx, 'Attachment', id, 'Body', function(err, stream) {
      if (err || !stream) return callback(err || new Error('No Attachment Stream'));
      stream.info = { 
        name:info.Name,
        type:info.ContentType || simpleMime(info.Name),
        size:info.BodyLength
      };
      callback(undefined, stream);
    });
  });
}

