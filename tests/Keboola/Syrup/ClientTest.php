<?php
namespace Keboola\Syrup\Tests;

use Guzzle\Http\Message\Response;
use Guzzle\Plugin\Mock\MockPlugin;
use Keboola\Syrup\Client;

class ClientTest extends \PHPUnit_Framework_TestCase
{

    /**
     * Test createJob method with config parameter
     */
    public function testCreateJobWithConfig()
    {
        $client = Client::factory(array(
            "token" => 'test',
            "runId" => 'runidtest'
        ));

        $mock = new MockPlugin();

        $mock->addResponse(new Response(200, array(
            'Location' => 'https://syrup.keboola.com/test-component/run',
            'Content-Type' => 'application/json'
        ), '
            {
                "id": "80429487",
                "url": "https://syrup-testing.keboola.com/queue/job/80429487",
                "status": "waiting"
            }'));

        $client->addSubscriber($mock);
        $client->createJob("test-component", array("config" => 1));
        $requests = $mock->getReceivedRequests();

        $this->assertCount(1, $requests);

        /**
         * @var $request \Guzzle\Http\Message\EntityEnclosingRequest
         */
        $request = $requests[0];
        $this->assertEquals("https://syrup.keboola.com/test-component/run", $request->getUrl());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals('{"config":"1"}', $request->getBody()->readLine());
        $this->assertEquals("test", $request->getHeaders()->get("x-storageapi-token"));
        $this->assertEquals("runidtest", $request->getHeaders()->get("x-kbc-runid"));
    }

    /**
     * test createJob method with configData parameter
     */
    public function testCreateJobWithConfigData()
    {
        $client = Client::factory(array(
            "token" => 'test',
            "runId" => 'runidtest'
        ));

        $mock = new MockPlugin();

        $mock->addResponse(new Response(200, array(
            'Location' => 'https://syrup.keboola.com/test-component/run',
            'Content-Type' => 'application/json'
        ), '
            {
                "id": "80429487",
                "url": "https://syrup-testing.keboola.com/queue/job/80429487",
                "status": "waiting"
            }'));

        $client->addSubscriber($mock);
        $client->createJob("test-component", array("configData" => array("var" => "val")));
        $requests = $mock->getReceivedRequests();

        $this->assertCount(1, $requests);

        /**
         * @var $request \Guzzle\Http\Message\EntityEnclosingRequest
         */
        $request = $requests[0];
        $this->assertEquals("https://syrup.keboola.com/test-component/run", $request->getUrl());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals('{"configData":{"var":"val"}}', $request->getBody()->readLine());
        $this->assertEquals("test", $request->getHeaders()->get("x-storageapi-token"));
        $this->assertEquals("runidtest", $request->getHeaders()->get("x-kbc-runid"));
    }

    /**
     * Test createJob method with config parameter
     */
    public function testCreateJobSuper()
    {
        $client = Client::factory(array(
            "token" => "test",
            "super" => "super",
            "runId" => 'runidtest'
        ));

        $mock = new MockPlugin();

        $mock->addResponse(new Response(200, array(
            'Location' => 'https://syrup.keboola.com/super/test-component/run',
            'Content-Type' => 'application/json'
        ), '
            {
                "id": "80429487",
                "url": "https://syrup-testing.keboola.com/queue/job/80429487",
                "status": "waiting"
            }'));

        $client->addSubscriber($mock);
        $client->createJob("test-component", array("config" => 1));
        $requests = $mock->getReceivedRequests();

        $this->assertCount(1, $requests);

        /**
         * @var $request \Guzzle\Http\Message\EntityEnclosingRequest
         */
        $request = $requests[0];
        $this->assertEquals("https://syrup.keboola.com/super/test-component/run", $request->getUrl());
        $this->assertEquals("POST", $request->getMethod());
        $this->assertEquals('{"config":"1"}', $request->getBody()->readLine());
        $this->assertEquals("test", $request->getHeaders()->get("x-storageapi-token"));
        $this->assertEquals("runidtest", $request->getHeaders()->get("x-kbc-runid"));
    }


    /**
     * test createJob method with configData parameter
     */
    public function testGetJob()
    {
        $client = Client::factory(array(
            "token" => 'test',
            "runId" => 'runidtest'
        ));

        $mock = new MockPlugin();

        $mock->addResponse(new Response(200, array(
            'Location' => 'https://syrup.keboola.com/queue/job/123456',
            'Content-Type' => 'application/json'
        ), '
           {
               "id": 123456,
               "status": "processing"
           }'));

        $client->addSubscriber($mock);
        $client->getJob(123456);
        $requests = $mock->getReceivedRequests();

        $this->assertCount(1, $requests);

        /**
         * @var $request \Guzzle\Http\Message\EntityEnclosingRequest
         */
        $request = $requests[0];
        $this->assertEquals("https://syrup.keboola.com/queue/job/123456", $request->getUrl());
        $this->assertEquals("GET", $request->getMethod());
        $this->assertEquals("test", $request->getHeaders()->get("x-storageapi-token"));
        $this->assertEquals("runidtest", $request->getHeaders()->get("x-kbc-runid"));
    }

    /**
     * test createJob method with configData parameter
     */
    public function testGetJobSuper()
    {
        $client = Client::factory(array(
            "token" => 'test',
            "super" => 'docker',
            "runId" => 'runidtest'
        ));

        $mock = new MockPlugin();

        $mock->addResponse(new Response(200, array(
            'Location' => 'https://syrup.keboola.com/queue/job/123456',
            'Content-Type' => 'application/json'
        ), '
           {
               "id": 123456,
               "status": "processing"
           }'));

        $client->addSubscriber($mock);
        $client->getJob(123456);
        $requests = $mock->getReceivedRequests();

        $this->assertCount(1, $requests);

        /**
         * @var $request \Guzzle\Http\Message\EntityEnclosingRequest
         */
        $request = $requests[0];
        $this->assertEquals("https://syrup.keboola.com/queue/job/123456", $request->getUrl());
        $this->assertEquals("GET", $request->getMethod());
        $this->assertEquals("test", $request->getHeaders()->get("x-storageapi-token"));
        $this->assertEquals("runidtest", $request->getHeaders()->get("x-kbc-runid"));
    }


    /**
     * test runJob method
     */
    public function testRunJob()
    {
        $client = Client::factory(array(
            "token" => 'test'
        ));

        $mock = new MockPlugin();

        $mock->addResponse(new Response(200, array(
            'Location' => 'https://syrup.keboola.com/test-component/run',
            'Content-Type' => 'application/json'
        ), '
            {
                "id": 123456,
                "url": "https://syrup-testing.keboola.com/queue/job/123456",
                "status": "waiting"
            }'));

        $mock->addResponse(new Response(200, array(
            'Location' => 'https://syrup.keboola.com/queue/job/123456',
            'Content-Type' => 'application/json'
        ), '
            {
                "id": 123456,
                "status": "processing"
            }'));

        $mock->addResponse(new Response(200, array(
            'Location' => 'https://syrup.keboola.com/queue/job/123456',
            'Content-Type' => 'application/json'
        ), '
            {
                "id": 123456,
                "status": "success"
            }'));

        $client->addSubscriber($mock);
        $response = $client->runJob("test-component", array("config" => 1));
        $requests = $mock->getReceivedRequests();

        $this->assertCount(3, $requests);
        $this->assertEquals("success", $response["status"]);
    }

    /**
     * Test max retries exception
     *
     * @expectedException \Keboola\Syrup\ClientException
     * @expectedExceptionMessage Too many retries.
     */
    public function testRunJobMaxTries()
    {
        $client = Client::factory(array(
            "token" => 'test'
        ));

        $reflection = new \ReflectionClass($client);
        $property = $reflection->getProperty('maxRetries');
        $property->setAccessible(true);
        $property->setValue($client, 1);

        $mock = new MockPlugin();

        $mock->addResponse(new Response(200, array(
            'Location' => 'https://syrup.keboola.com/test-component/run',
            'Content-Type' => 'application/json'
        ), '
            {
                "id": 123456,
                "url": "https://syrup-testing.keboola.com/queue/job/123456",
                "status": "waiting"
            }'));

        $mock->addResponse(new Response(200, array(
            'Location' => 'https://syrup.keboola.com/queue/job/123456',
            'Content-Type' => 'application/json'
        ), '
            {
                "id": 123456,
                "status": "processing"
            }'));

        $mock->addResponse(new Response(200, array(
            'Location' => 'https://syrup.keboola.com/queue/job/123456',
            'Content-Type' => 'application/json'
        ), '
            {
                "id": 123456,
                "status": "processing"
            }'));

        $client->addSubscriber($mock);
        $client->runJob("test-component", array("config" => 1));
    }

    /**
     * Test invalid response from api
     *
     * @expectedException \Keboola\Syrup\ClientException
     * @expectedExceptionMessage Invalid response.
     */
    public function testRunJobInvalidResponse()
    {
        $client = Client::factory(array(
            "token" => 'test'
        ));

        $mock = new MockPlugin();

        $mock->addResponse(new Response(200, array(
            'Location' => 'https://syrup.keboola.com/test-component/run',
            'Content-Type' => 'application/json'
        ), '
            {
                "something": "else"
            }'));

        $client->addSubscriber($mock);
        $client->runJob("test-component", array("config" => 1));
    }
}
