const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');
const Records = require('./records.model');
const { promisify } = require('util');

const unlinkAsync = promisify(fs.unlink);

// POST /upload
const upload = async (req, res) => {
    const { file } = req;

    if (!file) {
        return res.status(400).json({ message: 'No se subió ningún archivo.' });
    }

    const filePath = path.resolve(file.path);
    const records = [];
    const BATCH_SIZE = 1000;
    let insertedCount = 0;

    try {
        await new Promise((resolve, reject) => {
            const stream = fs.createReadStream(filePath)
                .pipe(csv.parse({ headers: true }))
                .on('error', reject)
                .on('data', async (row) => {
                    stream.pause();

                    records.push(row);

                    if (records.length >= BATCH_SIZE) {
                        stream.pause();
                        try {
                            await Records.insertMany(records);
                            insertedCount += records.length;
                            records.length = 0;
                        } catch (err) {
                            console.error('Error insertando batch:', err);
                        } finally {
                            stream.resume();
                        }
                    } else {
                        stream.resume();
                    }
                })
                .on('end', async () => {
                    try {
                        if (records.length > 0) {
                            await Records.insertMany(records);
                            insertedCount += records.length;
                        }
                        resolve();
                    } catch (err) {
                        reject(err);
                    }
                });
        });

        await unlinkAsync(filePath); // eliminar archivo temporal

        return res.status(200).json({
            message: `Archivo procesado con éxito. Total insertados: ${insertedCount}`
        });
    } catch (error) {
        console.error('Error procesando archivo:', error);
        await unlinkAsync(filePath);
        return res.status(500).json({ message: 'Error procesando el archivo.' });
    }
};

// GET /records
const list = async (_, res) => {
    try {
        const data = await Records.find({}).sort({ _id: -1 }).limit(10).lean();
        return res.status(200).json(data);
    } catch (err) {
        return res.status(500).json(err);
    }
};

module.exports = {
    upload,
    list,
};
