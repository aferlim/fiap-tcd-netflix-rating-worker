const client = require('request-promise-native')
var Kafka = require('no-kafka')
const errorFactory = require('error-factory')

const MovieError = errorFactory('MovieError')

var consumer = new Kafka.SimpleConsumer({
	connectionString:
		process.env.KAFKA_CONNECTION_STRING || 'kafka://127.0.0.1:9092'
})

var ProcessMessage = function(messageSet, topic, partition) {
	messageSet.forEach(function(m) {
		console.log(topic, partition, m.offset, m.message.value.toString('utf8'))

		var message = JSON.parse(m.message.value.toString('utf8'))

		MovieSync(partition, m.offset, message)
	})
}

const MovieSync = async (partition, offset, message) => {
	let options = {
		type: 'POST',
		uri: 'movies-api.service.consul:8083/rate',
		json: true,
		headers: {},
		body: message
	}
	try {
		//let movie = await client(options)

		console.log(options)
		Commit(partition, offset)

		//return movie
	} catch (error) {
		throw MovieError(error)
	}
}

const Commit = (partition, offset) => {
	consumer.commitOffset([
		{
			topic: 'rating-topic',
			partition: partition,
			offset: offset
		}
	])
}

const Start = () =>
	consumer.init().then(function() {
		return consumer.subscribe('rating-topic', [0, 1], ProcessMessage)
	})

Start()
