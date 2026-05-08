import pino from 'pino';
import {config} from '../config.js';

const pretty = process.env.NODE_ENV !== 'production';

export const logger = pino({
    level: config.logLevel,
    transport: pretty
        ? {
            target: 'pino-pretty',
            options: {colorize: true, translateTime: 'HH:MM:ss.l', ignore: 'pid,hostname'},
        }
        : undefined,
});
