module.exports = {
    "env": {
        "es6": true,
        "node": true
    },
    "extends": [
        "eslint:recommended",
        "plugin:node/recommended"
    ],
    "parserOptions": {
        "ecmaVersion": 2020
    },
    "plugins": [
    ],
    "rules": {
        "indent": [
            "error",
            4
        ],
        "brace-style": [
            "error",
            "allman",
            { "allowSingleLine": true }
        ],
        "linebreak-style": [
            "error",
            "unix"
        ],
        "semi": [
            "error",
            "always"
        ],
        "no-useless-escape": [
            "off"
        ],
        "no-control-regex": [
            "off"
        ],
        "no-empty": [
            "off"
        ],
        "no-process-exit": [
            "off"
        ],
        "node/shebang": [
            "off"
        ]
    }
};
