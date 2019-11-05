const chalk = require('chalk')

module.exports = {
	info(message, success = true) {
		const status = success ? chalk.bold.green('[ok]') : chalk.bold.red('[fail]')
		console.log(status, chalk.white(message))
	},
	error(message) {
		console.log(chalk.bold.red('[fail]'), chalk.black.bgRed(message))
	},
	newline() {
		console.log('\n\t')
	},
	plain(message) {
		console.log(message)
	}
}
