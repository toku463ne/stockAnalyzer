/**
 * Required External Modules
 */
const express = require("express");
const bodyParser = require('body-parser');
const path = require("path");
//const config = require('config');
const fs = require('fs');
const consts = require('./consts')
const chart = require('./routes/chart');
   
 /**
 * App Variables
 */
const app = express();
const server = require('http').createServer(app);
const port = process.env.PORT || "3000";

/**
 *  App Configuration
 */

app.set("views", path.join(__dirname, "views"));
app.set("view engine", "pug");
app.use(express.static(path.join(__dirname, "public")));
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());


/**
 * Routes Definitions
 */

app.use(express.static(path.join(__dirname, 'public')));

app.get("/", (req, res) => {
    let obj = JSON.parse(fs.readFileSync("./default_chart.json"));
    var chart_conf = JSON.stringify(obj, null, 4)
    //console.log(chart_conf)
    res.render("index", { "title": "Specify the chart", 
        "default_chart_conf": chart_conf});
});


app.post("/chart", (req, res) => {
    //console.log(req.body)
    //res.send("test")
    chart.showChart(req, res)
});

/**
 * Server Activation
 */
server.listen(port, () => {
    console.log(`Listening to requests on http://localhost:${port}`);
});

module.exports = app;