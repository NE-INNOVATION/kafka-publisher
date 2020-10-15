
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System;
using policy_issue.Model;
using System.Collections;
using System.Linq;
using policy_issue.Services;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using Microsoft.AspNetCore.Cors;

namespace policy_issue.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class PolicyController : ControllerBase
    {

        private readonly ILogger<PolicyController> _logger;
        private readonly KafkaConsumer _consumer;

        private static string MongoConnectionString = Environment.GetEnvironmentVariable("MONGO_CONNECTION") ?? "mongodb://lrqi_db:lrqi_db_pwd@lrqidb-shard-00-00-wksjy.mongodb.net:27017,lrqidb-shard-00-01-wksjy.mongodb.net:27017,lrqidb-shard-00-02-wksjy.mongodb.net:27017/test?authSource=admin&replicaSet=LRQIDB-shard-0&readPreference=primary&retryWrites=true&ssl=true";

        private static string MONGO_DB_NAME = Environment.GetEnvironmentVariable("MONGO_DB_NAME") ?? "lrqi";

        public PolicyController(ILogger<PolicyController> logger, KafkaConsumer consumer)
        {
            _logger = logger;
            _consumer =  consumer;
        }

        [HttpGet("config")]
        public string GetVariables()
        {
            string message="";

            foreach(DictionaryEntry e in System.Environment.GetEnvironmentVariables())
            {
                message += e.Key.ToString()  + "::" + e.Value.ToString() + "|||";
            }

            return message;

        }

        [HttpGet("policyData")]
        public string GetPolicyData([FromQuery] string quoteId)
        {
           var mongo = new MongoConnector(MongoConnectionString, MONGO_DB_NAME);
            var policyObject = mongo.GetPolicyObject(quoteId);
            return policyObject.ToString();

        }


        [HttpGet("message")]
        public string GetMessage([FromQuery] long time)
        {
            return _consumer.SetupConsume((time > 20000 || time == 0 )?4000 : time ).FirstOrDefault();
        }

        [HttpPost("publish")]
        public async Task<string> publishAsync([FromBody] Object requestBody)
        {
            _logger.LogInformation("Service called for publish");
            _logger.LogInformation($"Request Body called {requestBody.ToString()}");

            return await KafkaService.SendMessage(requestBody.ToString(), _logger);
        }

    }
}
