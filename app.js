var express = require("express");
var bodyParser = require("body-parser");
var app = express();
const path = require('path');
var mqttHandler = require('./mqtt_handler');

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }))

global.global_database_connected = false;
global.global_mqtt_sensors_list = [];

app.use(function(req, res, next){
  console.log('%s %s', req.method, req.url);
  next();
});

app.use(express.static(path.join(__dirname, 'build')))

app.use(function (req, res, next) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader('Content-Type', 'application/json');
  res.setHeader("Access-Control-Allow-Headers", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, PATCH, DELETE");
  next();
});

app.get('/', function (req, res) {
  res.sendFile(path.join(__dirname, 'build', 'index.html'));
})

const detectionRouter = require('./routes/detections');
const sensorsRouter = require('./routes/sensors');
app.use(detectionRouter);
app.use('/sensors', sensorsRouter);

app.listen(8080, function () {
  console.log("app running on port ", 8080);
});

exports.app = app;