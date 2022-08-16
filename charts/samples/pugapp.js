const express = require('express');
const app = express();

app.set('view engine', 'pug');

app.get('/', (req, res) => {
    res.render('sample', { message: 'Hello' });
});

app.listen('3000', () => {
    console.log('Application started');
});