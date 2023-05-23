const express = require('express');
const userController = require('../../controller/transfer/transferController');

const router = express.Router();

router.post('/transfer/send-messgae',
    userController.sendMessage
);


router.post('/transfer/received-messgae',
    userController.receivedMessgaeByTopic
);

module.exports.transferRoutes = router;
