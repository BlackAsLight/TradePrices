import { CsvParseStream, stringify } from 'https://deno.land/std@0.213.0/csv/mod.ts'
import { DOMParser, Element } from 'https://deno.land/x/deno_dom@v0.1.43/deno-dom-wasm.ts'
import { AsyncIter } from 'https://deno.land/x/iterstar@v1.1.2/mod.ts'
import { read } from 'https://deno.land/x/streaming_zip@v1.1.0/read.ts'

type Resources = 'oil' | 'coal' | 'iron' | 'bauxite' | 'lead' | 'uranium' | 'food'
	| 'gasoline' | 'steel' | 'aluminum' | 'munitions' | 'credits'

type RawTrade = {
	trade_id: string
	date_created: string
	offerer_nation_id: string
	receiver_nation_id: string
	offer_type: '0' | '1' | '2'
	buy_or_sell: 'buy' | 'sell'
	resource: Resources
	quantity: string
	price: string
	accepted: '1' | '0'
	original_trade_id: string
	date_accepted: string
}

type Row = { date: number } & Record<Resources, number>

declare global {
	interface Date {
		addDays: (x: number) => this
	}
}

Date.prototype.addDays = function (x) {
	this.setUTCDate(this.getUTCDate() + x)
	return this
}

const data = await (async function () {
	if (!await Deno.stat('./static/data.csv').then(() => true).catch(() => false))
		return {}
	const iter = new AsyncIter((await Deno.open('./static/data.csv'))
		.readable
		.pipeThrough(new TextDecoderStream())
		.pipeThrough(new CsvParseStream()))
	const keys = (await iter.shift())!
	return iter
		.map(values => values.reduce((obj, value, i) => (obj[ keys[ i ] ] = value, obj), {} as Record<string, string>))
		.reduce((obj, row) => {
			obj[ row.date ] = row
			return obj
		}, {} as Record<string, Record<Resources, string>>)
})()

await promiseManager(
	[
		...new DOMParser()
			.parseFromString(await (await fetch('https://politicsandwar.com/data/trades/?C=M;O=A')).text(), 'text/html')!
			.querySelectorAll('a[href*="trade"]') as unknown as Element[]
	]
		.map(aTag => new Date(aTag.getAttribute('href')!.slice(7, -8)).getTime()),
	async time => {
		const startTime = performance.now()
		let timeStamp = dateToTimeStamp(new Date(time))
		time = new Date(time).addDays(-1).getTime()
		if (data[ time ])
			return

		const iter = new AsyncIter(read(
			(await fetch(`https://politicsandwar.com/data/trades/trades-${timeStamp}.csv.zip`)).body!
		))
			.filter<{ type: 'file' }>(entry => entry.type === 'file')
			.map(entry => entry.body.stream().pipeThrough(new TextDecoderStream()).pipeThrough(new CsvParseStream()))
			.flat()
		timeStamp = dateToTimeStamp(new Date(time))

		const keys = (await iter.shift())!
		data[ time ] = Object.fromEntries(
			Object.entries(
				await iter
					.map(values => values.reduce((obj, value, i) => (obj[ keys[ i ] as 'trade_id' ] = value, obj), {} as RawTrade))
					.filter(trade => trade.offer_type === '0' && trade.accepted && dateToTimeStamp(new Date(trade.date_accepted.replace(' ', 'T') + 'Z')) === timeStamp)
					.reduce((sums, trade) => {
						sums[ trade.resource ][ 0 ] += +trade.price * +trade.quantity
						sums[ trade.resource ][ 1 ] += +trade.quantity
						return sums
					}, {
						oil: [ 0, 0 ],
						coal: [ 0, 0 ],
						iron: [ 0, 0 ],
						bauxite: [ 0, 0 ],
						lead: [ 0, 0 ],
						uranium: [ 0, 0 ],
						food: [ 0, 0 ],
						gasoline: [ 0, 0 ],
						steel: [ 0, 0 ],
						aluminum: [ 0, 0 ],
						munitions: [ 0, 0 ],
						credits: [ 0, 0 ]
					} satisfies Record<Resources, [ number, number ]>)
			)
				.map(([ key, value ]) => [ key, (Math.round(value[ 0 ] / value[ 1 ] * 100) / 100).toFixed(2) ])
		) as Record<Resources, string>
		const endTime = performance.now()
		console.log(`Processed ${timeStamp} in ${(endTime - startTime).toLocaleString('en-US', { maximumFractionDigits: 0 })}ms`)
	},
	10
)

const result = Object.entries(data).map(([ key, value ]) => ({ date: key, ...value }))
await Deno.writeTextFile('./static/data.csv', stringify(result, { columns: Object.keys(result[ 0 ]) }))

async function promiseManager<T, U>(
	input: T[],
	func: (cell: T) => Promise<U>,
	concurrent: number
): Promise<(U | undefined)[]> {
	const promises: Promise<number>[] = []
	const output: (U | undefined)[] = []
	for (let i = 0; i < Math.min(input.length, concurrent); ++i)
		promises.push(wrap(i, i))
	let j
	for (let i = concurrent; i < input.length; ++i) {
		j = await Promise.race(promises)
		promises.splice(j, 1, wrap(i, j))
	}
	await Promise.allSettled(promises)
	return output
	async function wrap(i: number, j: number) {
		output[ i ] = await func(input[ i ]).catch(error => (console.error(error), undefined))
		return j
	}
}

function dateToTimeStamp(date: Date): string {
	return date.getUTCFullYear()
		+ '-'
		+ (date.getUTCMonth() + 1).toString().padStart(2, '0')
		+ '-'
		+ date.getUTCDate().toString().padStart(2, '0')
}
