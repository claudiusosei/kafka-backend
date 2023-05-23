const express = require('express');
const { transferRoutes } = require('./transfer/transfer.route');

const router = express.Router();

router.use(transferRoutes);

module.exports = router;