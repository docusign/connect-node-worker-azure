#!/usr/bin/env node

/**
 * See settings in ds_configuration.js
 */

const dsConfig = require('./ds_configuration.js').config
    , { ServiceBusClient, ReceiveMode } = require("@azure/service-bus")  // See https://github.com/Azure/azure-sdk-for-js/tree/master/sdk/servicebus/service-bus/
    , processNotification = require('./lib/processNotification.js')
    , dsJwtAuth = require('./lib/dsJwtAuth')
    ;

const sleep = (seconds) => {
      return new Promise(resolve => setTimeout(resolve, 1000 * seconds))
}

/**
 * Process a message
 * See https://github.com/Azure/azure-sdk-for-js/tree/master/sdk/servicebus/service-bus#register-message-handler
 * @param {string} message
 */
const messageHandler = async function _messageHandler (message) {
  if (dsConfig.debug) {
    let m = `Processing message id ${message.messageId}`;
    console.log(`${new Date().toUTCString()} ${m}`);
  }

  if (message.body) {
    await processNotification.process(message.body.test, message.body.xml);
    message.complete();
  } else {
    let m = `Null body in message id ${message.messageId}. Ignoring.`;
    console.log(`${new Date().toUTCString()} ${m}`);
    message.deadLetter("Null body");
  }
}


/**
 * The function will listen forever, dispatching incoming notifications
 * to the processNotification library. 
 */
async function listenForever() {
  // Check that we can get a DocuSign token
  await testToken();

  let queue = null
    , ns = null
    , receiver = null
    , restart = true
  const connString = dsConfig.svcBusConnectionString
      , queueName = dsConfig.queueName;

  const startQueue = async () => {
    try {
      if (queue) {try {await queue.close()} catch (e) {}}
      if (   ns) {try {await    ns.close()} catch (e) {}}

      ns = ServiceBusClient.createFromConnectionString(connString);
      queue = ns.createQueueClient(queueName);
      receiver = queue.createReceiver(ReceiveMode.peekLock);

      const errorHandler = (e) => {
        console.log(`${new Date().toUTCString()} Exception while processing a message: ${e}`);
        throw new Error('Error from message handler');
      }
      receiver.registerMessageHandler(messageHandler, errorHandler, { autoComplete: false });

    } catch (e) {
      console.error(`\n${new Date().toUTCString()} Error while starting the queue:`);
      console.error(e);
      await sleep(5);
      restart = true;
    }
  }

  while (true) {
    if (restart) {
      console.log(`${new Date().toUTCString()} Starting queue worker`);
      await startQueue();
      await sleep(5);
      restart = false;
    }
    await sleep(5);
  }
}


/**
 * Check that we can get a DocuSign token and handle common error
 * cases: ds_configuration not configured, need consent.
 */
async function testToken() {  
  try {
    if (! dsConfig.clientId || dsConfig.clientId == '{CLIENT_ID}') {
      console.log (`
Problem: you need to configure this example, either via environment variables (recommended) 
         or via the ds_configuration.js file. 
         See the README file for more information\n\n`);
      process.exit();
    }
  
    await dsJwtAuth.checkToken();
  } catch (e) {
    let body = e.response && e.response.body;
    if (body) {
      // DocuSign API problem
      if (body.error && body.error == 'consent_required') {
        // Consent problem
        let consent_scopes = "signature%20impersonation",
            consent_url = `https://${dsConfig.authServer}/oauth/auth?response_type=code&` +
              `scope=${consent_scopes}&client_id=${dsConfig.clientId}&` +
              `redirect_uri=${dsConfig.oAuthConsentRedirectURI}`;
        console.log(`\nProblem:   C O N S E N T   R E Q U I R E D
    Ask the user who will be impersonated to run the following url:
        ${consent_url}
    
    It will ask the user to login and to approve access by your application.
    
    Alternatively, an Administrator can use Organization Administration to
    pre-approve one or more users.\n\n`)
        process.exit();
      } else {
        // Some other DocuSign API problem 
        console.log (`\nAPI problem: Status code ${e.response.status}, message body:
${JSON.stringify(body, null, 4)}\n\n`);
        process.exit();
      }  
    } else {
      // Not an API problem
      throw e;
    }
  }
}

/* The mainline...            */
/* Start listening for jobs   */
listenForever()
