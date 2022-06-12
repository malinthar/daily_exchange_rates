import ballerina/http;
import ballerina/io;
import wso2/choreo.sendsms;
import ballerina/task;
import ballerina/time;

configurable string apiKey = ?;

http:Client cli = check new ("https://api.apilayer.com/exchangerates_data");
sendsms:Client smsCli = check new ();

type ExchangeRate record {|
    string to;
    string 'from;
    string date;
    decimal rate;
|};

type Response record {
    boolean success;
    record {|
        string 'from;
        string to;
        decimal amount;
    |} query;
    string date;
    decimal result;
};

type Subscriber record {|
    readonly string mobileNumber;
    readonly Conversion[] conversions;
|};

type Conversion record {|
    string to;
    string 'from;
|};

function getCurrentRate(Conversion conversion, string date) returns Response|error {
    json response = check cli->get(string `/convert?to=${conversion.to}&from=${conversion.'from}&amount=1&date=${date}`, {apiKey});
    Response res = check response.cloneWithType(Response);
    return res;
}

class Job {
    *task:Job;
    private map<Subscriber> subscribers;

    isolated function init() {
        self.subscribers = {};
    }

    public function execute() {
        do {
            string date = string:substring(time:utcToString(time:utcNow()), 0, 10);
            check self.getCurrentRatesAndSendSms(date);
        } on fail error err {
            io:print(err);
        }
    }

    

    function getCurrentRatesAndSendSms(string date) returns error? {
        check from Subscriber subscriber in self.subscribers
            do {
                Response[] rates = [];
                foreach Conversion conversion in subscriber.conversions {
                    rates.push(check getCurrentRate(conversion, date));
                }
                string ratesRes = string `date:${date}\n`.toBalString() + rates.'map(
                    rate => string `to:${rate.query.'from}-${rate.query.to}:${rate.result}`.toBalString()).toBalString();
                string sendSmsResult = check smsCli->sendSms(subscriber.mobileNumber, ratesRes);
                io:println(sendSmsResult);
            };
    }

    isolated function addSubscriber(Subscriber subscriber) returns error? {
        if self.subscribers.hasKey(subscriber.mobileNumber) {
            return error("Alread subscribed!");
        }
        self.subscribers[subscriber.mobileNumber] = subscriber;
    }

    isolated function  removeSubscriber(string subscriber) returns string | error {
        if self.subscribers.hasKey(subscriber) {
            Subscriber removedSubscriber = self.subscribers.remove(subscriber);
            return string `The number ${removedSubscriber.mobileNumber} has been unsubscribed successfully!`;

        }
        return error("Subscriber does not exist!");
    }

}

listener http:Listener securedEP = new(9090,
    secureSocket = {
        key: {
            certFile: "./resource/certificate.crt",
            keyFile: "./resource/privateKey.key"
        }
    }
);


@http:ServiceConfig {
    auth: [{fileUserStoreConfig: {}, scopes: ["admin"]}]
}
isolated service /conversionRates on securedEP {
    private Job job;
    private task:JobId jobid;

    function init() returns error? {
        self.job = new Job();
        self.jobid = check task:scheduleJobRecurByFrequency(self.job, 60, maxCount = 30);
    }

    isolated resource function post subscribe(@http:Payload Subscriber payload) returns string {
        lock {
            error? res = self.job.addSubscriber(payload);
            if res is error {
                return res.message();
            }
            return "Subscribed Successfully!";
        }
    }

    isolated resource function post  unsubscribe(@http:Payload record {|string mobile;|} payload) returns string{
      lock {
            string|error res = self.job.removeSubscriber(payload.mobile);
            if res is error {
                return res.message();
            }
            return res;
        }
    }


    isolated resource function get terminateJob() returns task:Error? {
        lock {
            return task:unscheduleJob(self.jobid);
        }
    }
}
