<?php

namespace Keboola\Syrup\Tests;

use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\Syrup\Client;
use Keboola\Syrup\ClientException;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Console\Logger\ConsoleLogger;
use Symfony\Component\Console\Output\StreamOutput;

class ClientTest extends \PHPUnit_Framework_TestCase
{

    /**
     * Test createJob method with config parameter
     */
    public function testCreateJobWithConfig()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": "80429487",
                    "url": "https://syrup-testing.keboola.com/queue/job/80429487",
                    "status": "waiting"
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'runId' => 'runIdTest',
            // 'logger' => $log
            'handler' => $stack
        ]);
        $client->createJob("test-component", ["config" => 1]);

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://syrup.keboola.com/test-component/run", $request->getUri()->__toString());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals('{"config":1}', $request->getBody()->read(2000));
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals("runIdTest", $request->getHeader("x-kbc-runid")[0]);
        $this->assertEquals('Keboola Syrup PHP Client', $request->getHeader("user-agent")[0]);
        $this->assertEquals('application/json', $request->getHeader("content-type")[0]);
    }

    /**
     * test createJob method with configData parameter
     */
    public function testCreateJobWithConfigData()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": "80429487",
                    "url": "https://syrup-testing.keboola.com/queue/job/80429487",
                    "status": "waiting"
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'runId' => 'runIdTest',
            'url' => 'https://example.com',
            'userAgent' => 'testClient',
            'handler' => $stack
        ]);
        $client->createJob("test-component", ["configData" => ["var" => "val"]]);

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://example.com/test-component/run", $request->getUri()->__toString());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals('{"configData":{"var":"val"}}', $request->getBody()->read(2000));
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals("runIdTest", $request->getHeader("x-kbc-runid")[0]);
        $this->assertEquals('Keboola Syrup PHP Client - testClient', $request->getHeader("user-agent")[0]);
    }

    /**
     * test createJob method with configData parameter
     */
    public function testCreateJobWithTag()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": "80429487",
                    "url": "https://syrup-testing.keboola.com/queue/job/80429487",
                    "status": "waiting"
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'runId' => 'runIdTest',
            'url' => 'https://example.com',
            'userAgent' => 'testClient',
            'handler' => $stack
        ]);
        $client->createJob("test-component", ["configData" => ["var" => "val"], "tag" => "1.2.3"]);

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://example.com/test-component/run/tag/1.2.3", $request->getUri()->__toString());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals('{"configData":{"var":"val"}}', $request->getBody()->read(2000));
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals("runIdTest", $request->getHeader("x-kbc-runid")[0]);
        $this->assertEquals('Keboola Syrup PHP Client - testClient', $request->getHeader("user-agent")[0]);
    }

    /**
     * Test createJob method with config parameter with parent component
     */
    public function testCreateJobSuper()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": "80429487",
                    "url": "https://syrup-testing.keboola.com/queue/job/80429487",
                    "status": "waiting"
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'super' => 'super',
            'runId' => 'runIdTest',
            'handler' => $stack
        ]);
        $client->createJob("test-component", ["config" => 1]);

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://syrup.keboola.com/super/test-component/run", $request->getUri()->__toString());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals('{"config":1}', $request->getBody()->read(1000));
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals("runIdTest", $request->getHeader("x-kbc-runid")[0]);
    }

    public function testRunSyncAction()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "response": 1
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'super' => 'docker',
            'runId' => 'runIdTest',
            'handler' => $stack
        ]);
        $client->runSyncAction('https://docker-runner.keboola.com/', 'test-component', 'example', [
            'url' => 'https://example.com',
        ]);

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://docker-runner.keboola.com/docker/test-component/action/example", $request->getUri()->__toString());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals('{"configData":{"url":"https:\/\/example.com"}}', $request->getBody()->read(1000));
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals("runIdTest", $request->getHeader("x-kbc-runid")[0]);
    }

    /**
     * Test getJob method.
     */
    public function testGetJob()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                   "id": 123456,
                   "status": "processing"
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'runId' => 'runIdTest',
            'handler' => $stack,
        ]);
        $client->getJob(123456);

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://syrup.keboola.com/queue/job/123456", $request->getUri()->__toString());
        $this->assertEquals("GET", $request->getMethod());
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals("runIdTest", $request->getHeader("x-kbc-runid")[0]);
    }


    /**
     * Test getJob method with parent component.
     */
    public function testGetJobSuper()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                   "id": 123456,
                   "status": "processing"
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'runId' => 'runIdTest',
            'super' => 'docker',
            'handler' => $stack,
        ]);
        $client->getJob(123456);

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://syrup.keboola.com/queue/job/123456", $request->getUri()->__toString());
        $this->assertEquals("GET", $request->getMethod());
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals("runIdTest", $request->getHeader("x-kbc-runid")[0]);
    }


    /**
     * Test runJob method with different states.
     */
    public function testRunJob()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "url": "https://syrup-testing.keboola.com/queue/job/123456",
                    "status": "waiting"
                }'
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "status": "processing"
                }'
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "status": "success"
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'handler' => $stack
        ]);
        $response = $client->runJob("test-component", ["config" => 1]);

        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://syrup.keboola.com/test-component/run", $request->getUri()->__toString());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals('{"config":1}', $request->getBody()->read(1000));
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);

        $this->assertCount(3, $container);
        $this->assertEquals("success", $response["status"]);
    }


    /**
     * Test runAsyncAction method with a POST request
     */
    public function testRunAsyncActionPost()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "url": "https://syrup-testing.keboola.com/queue/job/123456",
                    "status": "waiting"
                }'
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "status": "processing"
                }'
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "status": "success"
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'handler' => $stack,
            'runId' => 'runIdTest',
            'super' => 'provisioning'
        ]);
        $response = $client->runAsyncAction("async/docker", "POST", ["body" => ["type" => "rstudio"]]);

        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://syrup.keboola.com/provisioning/async/docker", $request->getUri()->__toString());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals('{"type":"rstudio"}', $request->getBody()->read(1000));
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals("runIdTest", $request->getHeader("x-kbc-runid")[0]);

        $request = $container[1]['request'];
        $this->assertEquals("https://syrup.keboola.com/queue/job/123456", $request->getUri()->__toString());

        $request = $container[2]['request'];
        $this->assertEquals("https://syrup.keboola.com/queue/job/123456", $request->getUri()->__toString());


        $this->assertCount(3, $container);
        $this->assertEquals("success", $response["status"]);
    }


    /**
     * Test runAsyncAction method with a DELETE request
     */
    public function testRunAsyncActionDelete()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "url": "https://syrup-testing.keboola.com/queue/job/123456",
                    "status": "waiting"
                }'
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "status": "processing"
                }'
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "status": "success"
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'handler' => $stack,
            'runId' => 'runIdTest',
            'super' => 'provisioning'
        ]);
        $response = $client->runAsyncAction("async/docker/1", "DELETE");

        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://syrup.keboola.com/provisioning/async/docker/1", $request->getUri()->__toString());
        $this->assertEquals("DELETE", $request->getMethod());
        $this->assertEquals('[]', $request->getBody()->read(1000));
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals("runIdTest", $request->getHeader("x-kbc-runid")[0]);

        $this->assertCount(3, $container);
        $this->assertEquals("success", $response["status"]);
    }


    /**
     * Test job with temporary failures when querying for job results.
     */
    public function testRunJobFailuresTemporary()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "url": "https://syrup-testing.keboola.com/queue/job/123456",
                    "status": "waiting"
                }'
            ),
            new Response(500, ['Content-Type' => 'application/json'], ''),
            new Response(500, ['Content-Type' => 'application/json'], ''),
            new Response(500, ['Content-Type' => 'application/json'], ''),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "status": "success"
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'handler' => $stack
        ]);
        $response = $client->runJob("test-component", ["config" => 1]);

        $this->assertCount(5, $container);
        $this->assertEquals("success", $response["status"]);
    }


    /**
     * Test job with permanent failures when querying for job results.
     */
    public function testRunJobFailuresPermanent()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "url": "https://syrup-testing.keboola.com/queue/job/123456",
                    "status": "waiting"
                }'
            ),
            new Response(500, ['Content-Type' => 'application/json'], ''),
            new Response(500, ['Content-Type' => 'application/json'], ''),
            new Response(500, ['Content-Type' => 'application/json'], ''),
            new Response(500, ['Content-Type' => 'application/json'], ''),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "status": "success"
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'handler' => $stack,
            'backoffMaxTries' => 2
        ]);
        try {
            $client->runJob("test-component", ["config" => 1]);
            $this->fail("Out of tries must raise exception");
        } catch (ClientException $e) {
            $this->assertContains('500 Internal Server Error', $e->getMessage());
        }
    }


    /**
     * Test a job which has been run and killed.
     */
    public function testRunAndKillJob()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "url": "https://syrup-testing.keboola.com/queue/job/123456",
                    "status": "waiting"
                }'
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "status": "processing"
                }'
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "status": "terminating"
                }'
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "status": "terminated"
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'handler' => $stack
        ]);
        $response = $client->runJob("test-component", ["config" => 1]);

        $this->assertCount(4, $container);
        $this->assertEquals("terminated", $response["status"]);
    }

    /**
     * Test invalid response from API.
     *
     * @expectedException \Keboola\Syrup\ClientException
     * @expectedExceptionMessage Invalid response.
     */
    public function testRunJobInvalidResponse()
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], '{"something": "else"}')
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'handler' => $stack
        ]);
        $client->runJob("test-component", ["config" => 1]);
    }

    public function testMalformedResponse()
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'text/plain'], 'test')]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'runId' => 'runIdTest',
            'super' => 'docker',
            'handler' => $stack
        ]);
        try {
            $client->getJob('123');
            $this->fail("Invalid message body must raise exception");
        } catch (ClientException $e) {
            $this->assertContains('Unable to parse response body', $e->getMessage());
        }
    }

    public function testInternalClientError()
    {
        $client = new Client([
            'token' => 'test',
            'runId' => 'runIdTest',
            'super' => 'docker',
            'backoffMaxTries' => 1,
        ]);
        $reflection = new \ReflectionClass($client);
        $method = $reflection->getMethod('setUrl');
        $method->setAccessible(true);
        $method->invoke($client, 'asdfghjkl');
        $method = $reflection->getMethod('setQueueUrl');
        $method->setAccessible(true);
        $method->invoke($client, 'asdfghjkl');
        try {
            $client->getJob('123');
            $this->fail("Invalid request must raise exception.");
        } catch (ConnectException $e) {
            $this->assertContains('Could not resolve', $e->getMessage());
        }
    }

    /**
     * @dataProvider tokenProvider
     */
    public function testToken($expected, $options)
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'text/plain'],
                '{"id":347372343,"runId":"347372339.347372344","isFinished":true}'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $defaults = array_merge(['handler' => $stack], $options);
        $client = new Client($defaults);
        $client->getJob("123");

        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals($expected, $request->hasHeader("x-storageapi-token"));
    }

    public function tokenProvider()
    {
        return [
            [true, ['token' => '1234']],
            [false, []]
        ];
    }

    public function testLogger()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                   "id": 123456,
                   "status": "failed"
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $log = new TestLogger();
        $client = new Client([
            'token' => 'test',
            'runId' => 'runIdTest',
            'super' => 'docker',
            'handler' => $stack,
            'logger' => $log
        ]);

        $client->getJob('123');
        $this->assertCount(1, $container);
        self::assertTrue($log->hasInfoThatContains('Syrup PHP Client'));
        self::assertTrue($log->hasInfoThatContains('"GET  /1.1" 200'));
    }

    /**
     * Test runJob method with different states.
     */
    public function testCustomUrlCustomQueueUrl()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "url": "https://syrup-testing.keboola.com/queue/job/123456",
                    "status": "waiting"
                }'
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "status": "processing"
                }'
            ),
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": 123456,
                    "status": "success"
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'handler' => $stack,
            'url' => 'http://syrup.local',
            'queueUrl' => 'http://queue.local'
        ]);
        $response = $client->runJob("test-component", ["config" => 1]);

        $this->assertCount(3, $container);

        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("http://syrup.local/test-component/run", $request->getUri()->__toString());

        $request = $container[1]['request'];
        $this->assertEquals("http://queue.local/queue/job/123456", $request->getUri()->__toString());

        $request = $container[2]['request'];
        $this->assertEquals("http://queue.local/queue/job/123456", $request->getUri()->__toString());

        $this->assertEquals("success", $response["status"]);
    }

    public function testGetProjectStats()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                  "jobs": {
                    "durationSum": 23456
                  }
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'runId' => 'runIdTest',
            'handler' => $stack,
        ]);
        $response = $client->getStats();

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals('https://syrup.keboola.com/docker/stats/project', $request->getUri()->__toString());
        $this->assertEquals('GET', $request->getMethod());
        $this->assertEquals('test', $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals(23456, $response['jobs']['durationSum']);
    }

    public function testGetProjectDailyStats()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                  "jobs": [
                    {
                      "date": "2020-08-01",
                      "durationSum": 12
                    },
                    {
                      "date": "2020-08-02",
                      "durationSum": 0
                    },
                    {
                      "date": "2020-08-03",
                      "durationSum": 220
                    }
                  ]
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'runId' => 'runIdTest',
            'handler' => $stack,
        ]);
        $response = $client->getStatsDaily('2020-08-01', '2020-08-03');

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];

        $timezoneOffset = urlencode((new \DateTime('now'))->format('P'));
        $this->assertEquals(
            'https://syrup.keboola.com/docker/stats/project/daily?fromDate=2020-08-01&toDate=2020-08-03&timezoneOffset=' . $timezoneOffset,
            $request->getUri()->__toString()
        );
        $this->assertEquals('GET', $request->getMethod());
        $this->assertEquals('test', $request->getHeader("x-storageapi-token")[0]);
        $this->assertArrayHasKey('jobs', $response);
    }

    public function testResolveConfiguration()
    {
        $componentId = "fakeComponent";
        $configId = "123";
        $configVersion = "1";

        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '
                 { 
                   "id": "12345",
                   "name": "test-config",
                   "configuration": {
                     "parameters": {
                        "a": "foo",
                        "b": "bar",
                        "c": "baz"
                     },
                     "shared_code_row_ids": [],
                     "storage": {},
                     "processors": {
                        "before": [],
                        "after": []
                     },
                     "variables_id": "111",
                     "variables_values_id": "222",
                     "shared_code_id": "333"
                   }
                 }'
            ),
        ]);

        $variableValuesData = [
            'values' => [
                [
                    'name' => 'firstvar',
                    'value' => 'foo',
                ],
                [
                    'name' => 'secondvar',
                    'value' => 'bar',
                ],
                [
                    'name' => 'thirdvar',
                    'value' => 'baz',
                ],
            ],
        ];


        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'runId' => 'runIdTest',
            'handler' => $stack,
        ]);
        $response = $client->resolveConfiguration(
            $componentId,
            $configId,
            $configVersion,
            null,
            $variableValuesData
        );
        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals('https://syrup.keboola.com/docker/configuration/resolve', $request->getUri()->__toString());
        $this->assertEquals('POST', $request->getMethod());
        $this->assertEquals('test', $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals(
            '{"componentId":"fakeComponent","configId":"123","configVersion":"1","variableValuesData":{"values":[{"name":"firstvar","value":"foo"},{"name":"secondvar","value":"bar"},{"name":"thirdvar","value":"baz"}]}}',
            $request->getBody()->read($request->getBody()->getSize())
        );

        $this->assertEquals(
            [
                'parameters' => [
                    'a' => 'foo',
                    'b' => 'bar',
                    'c' => 'baz',
                ],
                'shared_code_row_ids' => [],
                'storage' => [],
                'processors' => [
                    'before' => [],
                    'after' => [],
                ],
                'variables_id' => "111",
                'variables_values_id' => "222",
                'shared_code_id' => "333",
            ],
            $response['configuration']
        );
    }

    /**
     * Test killJob method.
     */
    public function testKillJob()
    {
        $mock = new MockHandler([
            new Response(
                202,
                ['Content-Type' => 'application/json'],
                '{"message":"job termination request accepted for processing"}'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'handler' => $stack,
        ]);
        $client->killJob(123456);

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://syrup.keboola.com/queue/jobs/123456/kill", $request->getUri()->__toString());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);
    }

    public function testListJobs()
    {
        $mockResponse = '[{
            "id": 778350626,
            "runId": "778350627",
            "lockName": "docker-8786-keboola.ex-google-drive-763493370",
            "project": {
              "id": 8786,
              "name": "Miro - columns debug"
            },
            "token": {
              "id": "397969",
              "description": "miro@keboola.com"
            },
            "component": "docker",
            "command": "run",
            "params": {
              "config": "763493370",
              "component": "keboola.ex-google-drive",
              "mode": "run"
            },
            "result": {
              "message": "Job has been terminated"
            },
            "status": "terminated",
            "process": {
              "host": "ip-10-0-44-138.ec2.internal",
              "pid": 20146
            },
            "createdTime": "2021-11-15T20:12:19+01:00",
            "startTime": "2021-11-15T20:12:19+01:00",
            "endTime": "2021-11-15T20:12:27+01:00",
            "durationSeconds": 8,
            "waitSeconds": null,
            "nestingLevel": 0,
            "error": null,
            "errorNote": null,
            "terminatedBy": {
              "id": "397969",
              "description": "miro@keboola.com"
            },
            "encrypted": null,
            "usage": null,
            "_index": "prod_syrup_docker_2017_3",
            "_type": "jobs",
            "isFinished": true
          },
          {
            "id": 773441493,
            "runId": "773441494",
            "lockName": "docker-8786-keboola.ex-google-drive-763493370",
            "project": {
              "id": 8786,
              "name": "Miro - columns debug"
            },
            "token": {
              "id": "397969",
              "description": "miro@keboola.com"
            },
            "component": "docker",
            "command": "run",
            "params": {
              "config": "763493370",
              "component": "keboola.ex-google-drive",
              "mode": "run"
            },
            "result": {
              "message": "Job has been terminated"
            },
            "status": "terminated",
            "process": {
              "host": "ip-10-0-40-201.ec2.internal",
              "pid": 30989
            },
            "createdTime": "2021-11-04T09:55:39+01:00",
            "startTime": "2021-11-04T09:55:39+01:00",
            "endTime": "2021-11-04T09:55:45+01:00",
            "durationSeconds": 6,
            "waitSeconds": null,
            "nestingLevel": 0,
            "error": null,
            "errorNote": null,
            "terminatedBy": {
              "id": "397969",
              "description": "miro@keboola.com"
            },
            "encrypted": null,
            "usage": null,
            "_index": "prod_syrup_docker_2017_3",
            "_type": "jobs",
            "isFinished": true
          },
          {
            "id": 773441428,
            "runId": "773441429",
            "lockName": "docker-8786-keboola.ex-google-drive-763493370",
            "project": {
              "id": 8786,
              "name": "Miro - columns debug"
            },
            "token": {
              "id": "397969",
              "description": "miro@keboola.com"
            },
            "component": "docker",
            "command": "run",
            "params": {
              "config": "763493370",
              "component": "keboola.ex-google-drive",
              "mode": "run"
            },
            "result": {
              "message": "Job has been terminated"
            },
            "status": "terminated",
            "process": {
              "host": "ip-10-0-44-69.ec2.internal",
              "pid": 13852
            },
            "createdTime": "2021-11-04T09:55:14+01:00",
            "startTime": "2021-11-04T09:55:15+01:00",
            "endTime": "2021-11-04T09:55:21+01:00",
            "durationSeconds": 6,
            "waitSeconds": null,
            "nestingLevel": 0,
            "error": null,
            "errorNote": null,
            "terminatedBy": {
              "id": "397969",
              "description": "miro@keboola.com"
            },
            "encrypted": null,
            "usage": null,
            "_index": "prod_syrup_docker_2017_3",
            "_type": "jobs",
            "isFinished": true
          }]';

        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                $mockResponse
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = new Client([
            'token' => 'test',
            'handler' => $stack,
        ]);
        $client->listJobs();

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals('https://syrup.keboola.com/queue/jobs?q=&limit=100&offset=0', $request->getUri()->__toString());
        $this->assertEquals('GET', $request->getMethod());
        $this->assertEquals('test', $request->getHeader("x-storageapi-token")[0]);
    }
}
