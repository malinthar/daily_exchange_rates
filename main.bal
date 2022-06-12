import ballerina/http;
import wso2/choreo.sendsms;
import ballerina/task;
import ballerina/log;
import ballerina/time;

configurable string apiKey = ?;

const string BASE_URL = "https://api.apilayer.com/exchangerates_data";

http:Client cli = check new (BASE_URL);
sendsms:Client smsCli = check new ();

type ExchangeRate record {|
    string 'from;
    string to;
    string date;
    decimal rate;
|};

type Response record {
    boolean success;
    decimal result;
};

public type Subscriber record {|
    readonly string mobileNumber;
    readonly Conversion[] conversions;
|};

type Conversion record {|
    string to;
    string 'from;
|};

# Represents a job that can be executed as a task.
class Job {
    *task:Job;
    private map<Subscriber> subscribers;
    private map<string[]> conversions;

    isolated function init() {
        self.subscribers = {};
        self.conversions = {};
    }

    public function execute() {
        do {
            string date = string:substring(time:utcToString(time:utcNow()), 0, 10);
            check self.getRatesAndSendSms(date);
        } on fail error err {
            log:printError(err.message(), err);
        }
    }

    # Obtain the rates for each conversion and send messages to subscribers.
    #
    # + date - date for which the rates should be obtained
    # + return - error?
    function getRatesAndSendSms(string date) returns error? {
        log:printInfo(string `Starting the task on ${date}`);
        map<ExchangeRate[]> currentRates = {};

        log:printInfo("Calling api-layer");
        self.conversions.entries().forEach(function([string, string[]] entry) {
            string key = entry[0];
            string[] toCurencies = entry[1];

            ExchangeRate[] exchageRates = [];
            toCurencies.forEach(function(string to) {
                Response|error res = self.getRateForDate({'from: key, to}, date);
                if (res is error) {
                    return;
                }
                ExchangeRate rate = {
                    'from: key,
                    to,
                    rate: res.result,
                    date
                };
                exchageRates.push(rate);

            });
            currentRates[key] = exchageRates;
        });

        log:printInfo("Sending messages");
        self.subscribers.forEach(function(Subscriber subscriber) {
            ExchangeRate[] subscribedRates = [];
            subscriber.conversions.forEach(function(Conversion conversion) {
                if currentRates.hasKey(conversion.'from) {
                    ExchangeRate rate = currentRates.get(conversion.'from)
                            .filter(exchageRate => exchageRate.to.equalsIgnoreCaseAscii(conversion.to)).pop();
                    subscribedRates.push(rate);
                }
            });
            error? sendRatesViaSms = self.sendRatesViaSms(subscribedRates, date, subscriber);
            if sendRatesViaSms is error {
                log:printError(sendRatesViaSms.message());
            }
        });
        log:printInfo(string `Succesfully finished the task`);
    }

    # Add a conversion to the list of conversions if it is not present.
    #
    # + conversion - Parameter Description
    isolated function checkAndAddConversion(Conversion conversion) {
        if self.conversions.hasKey(conversion.'from) {
            self.conversions.get(conversion.'from).push(conversion.to);
            return;
        }
        self.conversions[conversion.'from] = [conversion.to];
    }

    # Call external service and get the coversion rate for a particular date.
    #
    # + conversion - conversion 
    # + date - date
    # + return - Response
    function getRateForDate(Conversion conversion, string date) returns Response|error {
        string path = string `/convert?to=${conversion.to}&from=${conversion.'from}&amount=1&date=${date}`;
        json res = check cli->get(path, {apiKey});
        return res.cloneWithType(Response);
    }

    # Send a SMS.
    #
    # + subscribedRates - set of conversions(rates)
    # + date - date  
    # + subscriber - subscriber
    # + return - error?
    function sendRatesViaSms(ExchangeRate[] subscribedRates, string date, Subscriber subscriber) returns error? {
        string ratesRes = string `Date:${date}`;
        subscribedRates.'map(rate => string `${rate.'from}-${rate.to}:${rate.rate}`)
                .forEach(function(string rate) {
            ratesRes = ratesRes + "\n" + rate;
        });
        _ = check smsCli->sendSms(subscriber.mobileNumber, ratesRes);
    }

    isolated function addSubscriber(Subscriber subscriber) returns error? {
        if self.subscribers.hasKey(subscriber.mobileNumber) {
            return error("Already subscribed!");
        }
        self.subscribers[subscriber.mobileNumber] = subscriber;
        check from Conversion conversion in subscriber.conversions
            do {
                if self.conversions.hasKey(conversion.'from) {
                    self.conversions.get(conversion.'from).push(conversion.to);
                    return;
                }
                self.conversions[conversion.'from] = [conversion.to];
            };
    }

    isolated function removeSubscriber(string subscriber) returns string|error {
        if self.subscribers.hasKey(subscriber) {
            Subscriber removedSubscriber = self.subscribers.remove(subscriber);
            return string `The number ${removedSubscriber.mobileNumber} has been unsubscribed successfully!`;

        }
        return error("Subscriber does not exist!");
    }

}

//HTTP listner on secure socket.
listener http:Listener securedEP = new (9090,
secureSocket = {
    key: {
        certFile: "./resource/certificate.crt",
        keyFile: "./resource/privateKey.key"
    }
}
);

# Conversions service.
isolated service /conversions on securedEP {
    private Job job;
    private task:JobId jobid;

    function init() returns error? {
        self.job = new Job();
        self.jobid = check task:scheduleJobRecurByFrequency(self.job, 60, maxCount = 30);
        log:printInfo("Conversions service initialized!");
    }

    # Subscriber to the conversions service
    #
    # + payload - subscriber details
    # + return - response message
    isolated resource function post subscribe(@http:Payload Subscriber payload) returns string {
        lock {
            error? res = self.job.addSubscriber(payload);
            if res is error {
                return res.message();
            }
            log:printInfo("New subscription");
            return "Subscribed to the conversions service successfully!";
        }
    }

    # Unsubscribe from the convetsions service
    #
    # + payload - mobile number
    # + return - response message
    isolated resource function post unsubscribe(@http:Payload record {|string mobile;|} payload) returns string {
        lock {
            string|error res = self.job.removeSubscriber(payload.mobile);
            if res is error {
                return res.message();
            }
            return "Unsubscribed from the conversions server Successfuly!";
        }
    }
}
