require("dotenv").config();
var express = require("express");
var bodyParser = require("body-parser");
var app = express();
const path = require('path');
// const databrix_client = require('./databricks');
const snowflake_client = require('./snowflake');
const cors = require('cors');

const corsOption = {
    origin: ['http://localhost:3000'],
};
app.use(cors(corsOption));




// databrix_client.initiate_connection();
snowflake_client.initiate_connection();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }))


const detectionRouter = require('./routes/detections');
app.use('/detections', detectionRouter);

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

app.listen(8080, function () {
  console.log("app running on port ", 8080);
});

exports.app = app;