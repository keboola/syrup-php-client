<?php

namespace Keboola\Syrup\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\Syrup\Client;
use Keboola\Syrup\ClientException;
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

        $client = Client::factory([
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

        $client = Client::factory([
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

        $client = Client::factory([
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

        $client = Client::factory([
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

        $client = Client::factory([
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

        $client = Client::factory([
            'token' => 'test',
            'handler' => $stack
        ]);
        $response = $client->runJob("test-component", ["config" => 1]);

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

        $client = Client::factory([
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

        $client = Client::factory([
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

        $client = Client::factory([
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

        $client = Client::factory([
            'token' => 'test',
            'handler' => $stack
        ]);
        $client->runJob("test-component", ["config" => 1]);
    }

    /**
     * Test encrypt plain string.
     */
    public function testEncryptString()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'text/plain'],
                'KBC::Encrypted==hgAYFu8FiztDlUOJ5Bg7cxoBKwOeNTONrv8Be/vsWMif3hW9dl8uunwuNvD4+c6ME0GHHjVCwRkgFvn3lD94PQ=='
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = Client::factory([
            'token' => 'test',
            'runId' => 'runIdTest',
            'handler' => $stack
        ]);
        $client->encryptString("docker", "test");

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://syrup.keboola.com/docker/encrypt", $request->getUri()->__toString());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals("text/plain", $request->getHeader("content-type")[0]);
        $this->assertEquals("runIdTest", $request->getHeader("x-kbc-runid")[0]);
        $this->assertEquals('test', $request->getBody());
    }


    /**
     * Test encrypt of a JSON array.
     */
    public function testEncryptData()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "plain": "test",
                    "#encrypted": "KBC::Encrypted==XbMxAbF29V+pKS2kG8OZkMdFjyAFc2bO+PUSE1q8qzYXPhGGLSzY+m4kTtJ5264mapanmlj1Gm95rvJMC+I0XQ=="
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = Client::factory([
            'token' => 'test',
            'runId' => 'runIdTest',
            'handler' => $stack
        ]);
        $client->encryptArray("docker", ["plain" => "test", "#encrypted" => "test"]);

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://syrup.keboola.com/docker/encrypt", $request->getUri()->__toString());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals("application/json", $request->getHeader("content-type")[0]);
        $this->assertEquals("runIdTest", $request->getHeader("x-kbc-runid")[0]);
        $this->assertEquals('{"plain":"test","#encrypted":"test"}', $request->getBody()->read(1000));
    }


    /**
     * Test encrypt of plain string with parent component.
     */
    public function testEncryptStringWithSuper()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                'KBC::ComponentEncrypted==U6pdFDMt/Di7cc1ySnWkSkUi1UGpmOnDMxb6+VXlZulITBOyz5X1UciP1IZkFHTN0Ckw1ERBcK8bjRN9Vz/Q7lts2ig8ENjd5oO7ue1HqWj00Ea0/xNZvFlh0f5TOqY2'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = Client::factory([
            'token' => 'test',
            'runId' => 'runIdTest',
            'super' => 'docker',
            'handler' => $stack
        ]);
        $client->encryptString("demo", "test");

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://syrup.keboola.com/docker/demo/encrypt", $request->getUri()->__toString());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals("text/plain", $request->getHeader("content-type")[0]);
        $this->assertEquals("runIdTest", $request->getHeader("x-kbc-runid")[0]);
        $this->assertEquals('test', $request->getBody()->read(1000));
    }


    /**
     * Test encrypt array with parent component and additional path.
     */
    public function testDataWithSuperAndSubPath()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'text/plain'],
                '{
                    "plain": "test",
                    "#encrypted": "KBC::Encrypted==XbMxAbF29V+pKS2kG8OZkMdFjyAFc2bO+PUSE1q8qzYXPhGGLSzY+m4kTtJ5264mapanmlj1Gm95rvJMC+I0XQ=="
                }'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = Client::factory([
            'token' => 'test',
            'runId' => 'runIdTest',
            'super' => 'docker',
            'handler' => $stack
        ]);
        $client->encryptArray("demo", ["plain" => "test", "#encrypted" => "test"], ["path" => "configs"]);

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://syrup.keboola.com/docker/demo/configs/encrypt", $request->getUri()->__toString());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals("application/json", $request->getHeader("content-type")[0]);
        $this->assertEquals("runIdTest", $request->getHeader("x-kbc-runid")[0]);
        $this->assertEquals('{"plain":"test","#encrypted":"test"}', $request->getBody()->read(1000));
    }


    /**
     * Test encrypt plain string with parent component and additional path.
     */
    public function testEncryptStringWithSuperAndSubPath()
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'text/plain'],
                'KBC::ComponentProjectEncrypted==Z05QovYpXTZN/DeHyXlarISB3Ca7Zs/ORW6fqs5EsIZaq7CZuHx9tAZFIxQHjd4HS15FuIcHC2Ko/q70MfH94t9TF7chg6zRDK7rOugSUpIvTXRelGHTkLzWkwiwd419'
            )
        ]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = Client::factory([
            'token' => 'test',
            'runId' => 'runIdTest',
            'super' => 'docker',
            'handler' => $stack
        ]);
        $client->encryptString("demo", "test", ["path" => "configs"]);

        $this->assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        $this->assertEquals("https://syrup.keboola.com/docker/demo/configs/encrypt", $request->getUri()->__toString());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals("test", $request->getHeader("x-storageapi-token")[0]);
        $this->assertEquals("text/plain", $request->getHeader("content-type")[0]);
        $this->assertEquals("runIdTest", $request->getHeader("x-kbc-runid")[0]);
        $this->assertEquals('test', $request->getBody());
    }


    public function testMalformedResponse()
    {
        $mock = new MockHandler([new Response(200, ['Content-Type' => 'text/plain'], 'test')]);

        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = Client::factory([
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
        $client = Client::factory([
            'token' => 'test',
            'runId' => 'runIdTest',
            'super' => 'docker',
        ]);
        $reflection = new \ReflectionClass($client);
        $method = $reflection->getMethod('setUrl');
        $method->setAccessible(true);
        $method->invoke($client, 'omfg::/mfg.mfg');
        try {
            $client->getJob('123');
            $this->fail("Invalid request must raise exception.");
        } catch (ClientException $e) {
            $this->assertContains('Could not resolve host', $e->getMessage());
        }
        try {
            $client->encryptArray('demo', ['#encrypt' => 'test']);
            $this->fail("Invalid request must raise exception.");
        } catch (ClientException $e) {
            $this->assertContains('Could not resolve host', $e->getMessage());
        }
        try {
            $client->encryptString('demo', 'test');
            $this->fail("Invalid request must raise exception.");
        } catch (ClientException $e) {
            $this->assertContains('Could not resolve host', $e->getMessage());
        }
    }


    public function testToken()
    {
        try {
            Client::factory([
                'runId' => 'runIdTest',
                'super' => 'docker',
            ]);
            $this->fail("Empty token must raise exception.");
        } catch (\InvalidArgumentException $e) {
            $this->assertContains(' token must be set', $e->getMessage());
        }
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

        $stream = fopen('data:text/plain,log', 'rw');
        $streamOutput = new StreamOutput($stream, StreamOutput::VERBOSITY_VERY_VERBOSE);
        $log = new ConsoleLogger($streamOutput);
        $client = Client::factory([
            'token' => 'test',
            'runId' => 'runIdTest',
            'super' => 'docker',
            'handler' => $stack,
            'logger' => $log
        ]);

        $client->getJob('123');
        $this->assertCount(1, $container);
        fseek($stream, 0);
        $data = fread($stream, 1000);
        $this->assertContains('Syrup PHP Client', $data);
        $this->assertContains('GET', $data);
    }
}
