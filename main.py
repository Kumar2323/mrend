import os
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from motor.motor_asyncio import AsyncIOMotorClient
from bson.json_util import dumps, loads
from bson.objectid import ObjectId
import json
import asyncio
from aiohttp import web

API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

app = Client("advanced_mongodb_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user_sessions = {}

async def get_database_names(mongo_client):
    return await mongo_client.list_database_names()

async def get_collection_names(mongo_client, db_name):
    db = mongo_client[db_name]
    return await db.list_collection_names()

async def get_documents(mongo_client, db_name, collection_name, limit=5, skip=0, query=None):
    db = mongo_client[db_name]
    collection = db[collection_name]
    if query:
        cursor = collection.find(query).skip(skip).limit(limit)
    else:
        cursor = collection.find().skip(skip).limit(limit)
    documents = await cursor.to_list(length=limit)
    return documents

async def get_document_count(mongo_client, db_name, collection_name, query=None):
    db = mongo_client[db_name]
    collection = db[collection_name]
    if query:
        return await collection.count_documents(query)
    return await collection.count_documents({})

async def insert_document(mongo_client, db_name, collection_name, document):
    db = mongo_client[db_name]
    collection = db[collection_name]
    result = await collection.insert_one(document)
    return result.inserted_id

async def update_document(mongo_client, db_name, collection_name, filter_query, update_data):
    db = mongo_client[db_name]
    collection = db[collection_name]
    result = await collection.update_one(filter_query, {"$set": update_data})
    return result.modified_count

async def delete_document(mongo_client, db_name, collection_name, filter_query):
    db = mongo_client[db_name]
    collection = db[collection_name]
    result = await collection.delete_one(filter_query)
    return result.deleted_count

async def delete_all_documents(mongo_client, db_name, collection_name):
    db = mongo_client[db_name]
    collection = db[collection_name]
    result = await collection.delete_many({})
    return result.deleted_count

async def delete_collection(mongo_client, db_name, collection_name):
    db = mongo_client[db_name]
    await db.drop_collection(collection_name)

async def delete_database(mongo_client, db_name):
    await mongo_client.drop_database(db_name)

async def create_collection(mongo_client, db_name, collection_name):
    db = mongo_client[db_name]
    await db.create_collection(collection_name)

async def split_and_send_message(client, chat_id, text, reply_markup=None):
    max_length = 4096
    if len(text) <= max_length:
        await client.send_message(chat_id, text, reply_markup=reply_markup)
    else:
        parts = [text[i:i+max_length] for i in range(0, len(text), max_length)]
        for i, part in enumerate(parts):
            if i == len(parts) - 1:
                await client.send_message(chat_id, part, reply_markup=reply_markup)
            else:
                await client.send_message(chat_id, part)

@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    await message.reply_text("Welcome to the Advanced MongoDB Management Bot!\nPlease enter your MongoDB URL to begin.")
    user_sessions[message.from_user.id] = {"state": "awaiting_mongo_url"}

@app.on_message(filters.text & ~filters.command("start"))
async def handle_text_input(client, message: Message):
    user_id = message.from_user.id
    if user_id in user_sessions:
        session = user_sessions[user_id]
        state = session.get("state")
        try:
            if state == "awaiting_mongo_url":
                mongo_url = message.text
                mongo_client = AsyncIOMotorClient(mongo_url)
                await mongo_client.server_info()  # Test the connection
                session["mongo_client"] = mongo_client
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Manage Databases", callback_data="manage_databases")],
                    [InlineKeyboardButton("Manage Collections", callback_data="manage_collections")],
                    [InlineKeyboardButton("Manage Documents", callback_data="manage_documents")]
                ])
                await message.reply_text("Connected successfully! Please select an option:", reply_markup=keyboard)
                session["state"] = "main_menu"
            elif state == "awaiting_new_db_name":
                new_db_name = message.text
                await create_collection(session["mongo_client"], new_db_name, "dummy_collection")
                await message.reply_text(f"Database '{new_db_name}' has been created.")
            elif state == "awaiting_new_coll_name":
                db_name = session["db"]
                new_coll_name = message.text
                await create_collection(session["mongo_client"], db_name, new_coll_name)
                await message.reply_text(f"Collection '{new_coll_name}' has been created in database '{db_name}'.")
            elif state == "awaiting_search":
                db_name = session["db"]
                coll_name = session["coll"]
                query = json.loads(message.text)
                documents = await get_documents(session["mongo_client"], db_name, coll_name, query=query)
                response = f"Search results in {db_name}.{coll_name}:\n\n"
                for doc in documents:
                    response += json.dumps(json.loads(dumps(doc)), indent=2) + "\n\n"
                await split_and_send_message(client, message.chat.id, response)
            elif state == "awaiting_insert":
                db_name = session["db"]
                coll_name = session["coll"]
                document = json.loads(message.text)
                inserted_id = await insert_document(session["mongo_client"], db_name, coll_name, document)
                await message.reply_text(f"Document inserted successfully. Inserted ID: {inserted_id}")
            elif state == "awaiting_update_filter":
                session["update_filter"] = json.loads(message.text)
                session["state"] = "awaiting_update_data"
                await message.reply_text("Now enter the update data in JSON format.\nExample: {\"$set\": {\"age\": 31}}")
                return
            elif state == "awaiting_update_data":
                db_name = session["db"]
                coll_name = session["coll"]
                update_filter = session["update_filter"]
                update_data = json.loads(message.text)
                modified_count = await update_document(session["mongo_client"], db_name, coll_name, update_filter, update_data)
                await message.reply_text(f"Update complete. Modified {modified_count} document(s).")
            elif state == "awaiting_delete":
                db_name = session["db"]
                coll_name = session["coll"]
                delete_filter = json.loads(message.text)
                deleted_count = await delete_document(session["mongo_client"], db_name, coll_name, delete_filter)
                await message.reply_text(f"Delete operation complete. Deleted {deleted_count} document(s).")
        except json.JSONDecodeError:
            await message.reply_text("Invalid JSON format. Please try again.")
        except Exception as e:
            await message.reply_text(f"An error occurred: {str(e)}")
        finally:
            if state!= "awaiting_update_data" and state!= "main_menu":
                session["state"] = "main_menu"
    else:
        await message.reply_text("Please use the /start command to begin.")

@app.on_callback_query(filters.regex("^manage_databases$"))
async def manage_databases(client, callback_query: CallbackQuery):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("List Databases", callback_data="list_databases")],
        [InlineKeyboardButton("Create Database", callback_data="create_database")],
        [InlineKeyboardButton("Delete Database", callback_data="delete_database")],
        [InlineKeyboardButton("Back to Main Menu", callback_data="main_menu")]
    ])
    await callback_query.edit_message_text("Database Management Options:", reply_markup=keyboard)

@app.on_callback_query(filters.regex("^manage_collections$"))
async def manage_collections(client, callback_query: CallbackQuery):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("List Collections", callback_data="list_collections")],
        [InlineKeyboardButton("Create Collection", callback_data="create_collection")],
        [InlineKeyboardButton("Delete Collection", callback_data="delete_collection")],
        [InlineKeyboardButton("Back to Main Menu", callback_data="main_menu")]
    ])
    await callback_query.edit_message_text("Collection Management Options:", reply_markup=keyboard)

@app.on_callback_query(filters.regex("^manage_documents$"))
async def manage_documents(client, callback_query: CallbackQuery):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("View Documents", callback_data="view_documents")],
        [InlineKeyboardButton("Search Documents", callback_data="search_documents")],
        [InlineKeyboardButton("Insert Document", callback_data="insert_document")],
        [InlineKeyboardButton("Update Document", callback_data="update_document")],
        [InlineKeyboardButton("Delete Document", callback_data="delete_document")],
        [InlineKeyboardButton("Delete All Documents", callback_data="delete_all_documents")],
        [InlineKeyboardButton("Back to Main Menu", callback_data="main_menu")]
    ])
    await callback_query.edit_message_text("Document Management Options:", reply_markup=keyboard)

@app.on_callback_query(filters.regex("^list_databases$"))
async def list_databases_callback(client, callback_query: CallbackQuery):
    mongo_client = user_sessions[callback_query.from_user.id]["mongo_client"]
    databases = await get_database_names(mongo_client)
    keyboard = []
    for db in databases:
        keyboard.append([InlineKeyboardButton(db, callback_data=f"db:{db}")])
    keyboard.append([InlineKeyboardButton("Back to Database Management", callback_data="manage_databases")])
    await callback_query.edit_message_text("Select a database:", reply_markup=InlineKeyboardMarkup(keyboard))

@app.on_callback_query(filters.regex("^create_database$"))
async def create_database_prompt(client, callback_query: CallbackQuery):
    user_sessions[callback_query.from_user.id]["state"] = "awaiting_new_db_name"
    await callback_query.edit_message_text("Please enter the name for the new database:")

@app.on_callback_query(filters.regex("^delete_database$"))
async def delete_database_prompt(client, callback_query: CallbackQuery):
    mongo_client = user_sessions[callback_query.from_user.id]["mongo_client"]
    databases = await get_database_names(mongo_client)
    keyboard = []
    for db in databases:
        keyboard.append([InlineKeyboardButton(db, callback_data=f"confirm_delete_db:{db}")])
    keyboard.append([InlineKeyboardButton("Cancel", callback_data="manage_databases")])
    await callback_query.edit_message_text("Select a database to delete:", reply_markup=InlineKeyboardMarkup(keyboard))

@app.on_callback_query(filters.regex("^confirm_delete_db:"))
async def confirm_delete_database(client, callback_query: CallbackQuery):
    db_name = callback_query.data.split(":")[1]
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Yes, delete database", callback_data=f"execute_delete_db:{db_name}")],
        [InlineKeyboardButton("No, cancel", callback_data="manage_databases")]
    ])
    await callback_query.edit_message_text(f"Are you sure you want to delete the database '{db_name}'? This action cannot be undone.", reply_markup=keyboard)

@app.on_callback_query(filters.regex("^execute_delete_db:"))
async def execute_delete_database(client, callback_query: CallbackQuery):
    db_name = callback_query.data.split(":")[1]
    mongo_client = user_sessions[callback_query.from_user.id]["mongo_client"]
    await delete_database(mongo_client, db_name)
    await callback_query.edit_message_text(f"Database '{db_name}' has been deleted.")

@app.on_callback_query(filters.regex("^db:"))
async def list_collections_callback(client, callback_query: CallbackQuery):
    db_name = callback_query.data.split(":")[1]
    mongo_client = user_sessions[callback_query.from_user.id]["mongo_client"]
    collections = await get_collection_names(mongo_client, db_name)
    keyboard = []
    for coll in collections:
        keyboard.append([InlineKeyboardButton(coll, callback_data=f"coll:{db_name}:{coll}")])
    keyboard.append([InlineKeyboardButton("Back to Databases", callback_data="list_databases")])
    await callback_query.edit_message_text(f"Collections in {db_name}:", reply_markup=InlineKeyboardMarkup(keyboard))

@app.on_callback_query(filters.regex("^create_collection$"))
async def create_collection_prompt(client, callback_query: CallbackQuery):
    mongo_client = user_sessions[callback_query.from_user.id]["mongo_client"]
    databases = await get_database_names(mongo_client)
    keyboard = []
    for db in databases:
        keyboard.append([InlineKeyboardButton(db, callback_data=f"new_coll_db:{db}")])
    keyboard.append([InlineKeyboardButton("Cancel", callback_data="manage_collections")])
    await callback_query.edit_message_text("Select a database for the new collection:", reply_markup=InlineKeyboardMarkup(keyboard))

@app.on_callback_query(filters.regex("^new_coll_db:"))
async def new_collection_name_prompt(client, callback_query: CallbackQuery):
    db_name = callback_query.data.split(":")[1]
    user_sessions[callback_query.from_user.id].update({"state": "awaiting_new_coll_name", "db": db_name})
    await callback_query.edit_message_text(f"Please enter the name for the new collection in database '{db_name}':")

@app.on_callback_query(filters.regex("^delete_collection$"))
async def delete_collection_prompt(client, callback_query: CallbackQuery):
    mongo_client = user_sessions[callback_query.from_user.id]["mongo_client"]
    databases = await get_database_names(mongo_client)
    keyboard = []
    for db in databases:
        keyboard.append([InlineKeyboardButton(db, callback_data=f"del_coll_db:{db}")])
    keyboard.append([InlineKeyboardButton("Cancel", callback_data="manage_collections")])
    await callback_query.edit_message_text("Select a database to delete a collection from:", reply_markup=InlineKeyboardMarkup(keyboard))

@app.on_callback_query(filters.regex("^del_coll_db:"))
async def delete_collection_select(client, callback_query: CallbackQuery):
    db_name = callback_query.data.split(":")[1]
    mongo_client = user_sessions[callback_query.from_user.id]["mongo_client"]
    collections = await get_collection_names(mongo_client, db_name)
    keyboard = []
    for coll in collections:
        keyboard.append([InlineKeyboardButton(coll, callback_data=f"confirm_delete_coll:{db_name}:{coll}")])
    keyboard.append([InlineKeyboardButton("Cancel", callback_data="manage_collections")])
    await callback_query.edit_message_text(f"Select a collection to delete from database '{db_name}':", reply_markup=InlineKeyboardMarkup(keyboard))

@app.on_callback_query(filters.regex("^confirm_delete_coll:"))
async def confirm_delete_collection(client, callback_query: CallbackQuery):
    db_name, coll_name = callback_query.data.split(":")[1:]
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Yes, delete collection", callback_data=f"execute_delete_coll:{db_name}:{coll_name}")],
        [InlineKeyboardButton("No, cancel", callback_data=f"del_coll_db:{db_name}")]
    ])
    await callback_query.edit_message_text(f"Are you sure you want to delete the collection '{coll_name}' from database '{db_name}'? This action cannot be undone.", reply_markup=keyboard)

@app.on_callback_query(filters.regex("^execute_delete_coll:"))
async def execute_delete_collection(client, callback_query: CallbackQuery):
    db_name, coll_name = callback_query.data.split(":")[1:]
    mongo_client = user_sessions[callback_query.from_user.id]["mongo_client"]
    await delete_collection(mongo_client, db_name, coll_name)
    await callback_query.edit_message_text(f"Collection '{coll_name}' has been deleted from database '{db_name}'.")

@app.on_callback_query(filters.regex("^coll:"))
async def show_collection_options(client, callback_query: CallbackQuery):
    _, db_name, coll_name = callback_query.data.split(":")
    keyboard = [
        [InlineKeyboardButton("View Documents", callback_data=f"view:{db_name}:{coll_name}:0")],
        [InlineKeyboardButton("Search Documents", callback_data=f"search:{db_name}:{coll_name}")],
        [InlineKeyboardButton("Insert Document", callback_data=f"insert:{db_name}:{coll_name}")],
        [InlineKeyboardButton("Update Document", callback_data=f"update:{db_name}:{coll_name}")],
        [InlineKeyboardButton("Delete Document", callback_data=f"delete:{db_name}:{coll_name}")],
        [InlineKeyboardButton("Delete All Documents", callback_data=f"delete_all:{db_name}:{coll_name}")],
        [InlineKeyboardButton("Back to Collections", callback_data=f"db:{db_name}")]
    ]
    await callback_query.edit_message_text(f"Options for {db_name}.{coll_name}:", reply_markup=InlineKeyboardMarkup(keyboard))

@app.on_callback_query(filters.regex("^view:"))
async def view_documents(client, callback_query: CallbackQuery):
    _, db_name, coll_name, skip = callback_query.data.split(":")
    skip = int(skip)
    mongo_client = user_sessions[callback_query.from_user.id]["mongo_client"]
    documents = await get_documents(mongo_client, db_name, coll_name, limit=5, skip=skip)
    total_docs = await get_document_count(mongo_client, db_name, coll_name)

    response = f"Documents in {db_name}.{coll_name} (Showing {skip+1}-{min(skip+5, total_docs)} of {total_docs}):\n\n"
    for doc in documents:
        response += json.dumps(json.loads(dumps(doc)), indent=2) + "\n\n"

    keyboard = []
    if skip > 0:
        keyboard.append([InlineKeyboardButton("Previous", callback_data=f"view:{db_name}:{coll_name}:{max(0, skip-5)}")])
    if skip + 5 < total_docs:
        keyboard.append([InlineKeyboardButton("Next", callback_data=f"view:{db_name}:{coll_name}:{skip+5}")])
    keyboard.append([InlineKeyboardButton("Back to Collection Options", callback_data=f"coll:{db_name}:{coll_name}")])

    await split_and_send_message(
        client,
        callback_query.message.chat.id,
        response,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    await callback_query.answer()

@app.on_callback_query(filters.regex("^search:"))
async def search_prompt(client, callback_query: CallbackQuery):
    _, db_name, coll_name = callback_query.data.split(":")
    user_sessions[callback_query.from_user.id].update({"db": db_name, "coll": coll_name, "state": "awaiting_search"})
    await callback_query.edit_message_text(
        f"Please enter your search query for {db_name}.{coll_name} in JSON format.\n"
        "Example: {\"name\": \"John\"}"
    )

@app.on_callback_query(filters.regex("^insert:"))
async def insert_prompt(client, callback_query: CallbackQuery):
    _, db_name, coll_name = callback_query.data.split(":")
    user_sessions[callback_query.from_user.id].update({"db": db_name, "coll": coll_name, "state": "awaiting_insert"})
    await callback_query.edit_message_text(
        f"Please enter the document to insert into {db_name}.{coll_name} in JSON format.\n"
        "Example: {\"name\": \"John\", \"age\": 30}"
    )

@app.on_callback_query(filters.regex("^update:"))
async def update_prompt(client, callback_query: CallbackQuery):
    _, db_name, coll_name = callback_query.data.split(":")
    user_sessions[callback_query.from_user.id].update({"db": db_name, "coll": coll_name, "state": "awaiting_update_filter"})
    await callback_query.edit_message_text(
        f"Please enter the filter to select the document to update in {db_name}.{coll_name} in JSON format.\n"
        "Example: {\"name\": \"John\"}"
    )

@app.on_callback_query(filters.regex("^delete:"))
async def delete_prompt(client, callback_query: CallbackQuery):
    _, db_name, coll_name = callback_query.data.split(":")
    user_sessions[callback_query.from_user.id].update({"db": db_name, "coll": coll_name, "state": "awaiting_delete"})
    await callback_query.edit_message_text(
        f"Please enter the filter to select the document to delete from {db_name}.{coll_name} in JSON format.\n"
        "Example: {\"name\": \"John\"}"
    )

@app.on_callback_query(filters.regex("^delete_all:"))
async def confirm_delete_all(client, callback_query: CallbackQuery):
    _, db_name, coll_name = callback_query.data.split(":")
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Yes, delete all", callback_data=f"execute_delete_all:{db_name}:{coll_name}")],
        [InlineKeyboardButton("No, cancel", callback_data=f"coll:{db_name}:{coll_name}")]
    ])
    await callback_query.edit_message_text(
        f"Are you sure you want to delete all documents from {db_name}.{coll_name}? This action cannot be undone.",
        reply_markup=keyboard
    )

@app.on_callback_query(filters.regex("^execute_delete_all:"))
async def execute_delete_all(client, callback_query: CallbackQuery):
    _, db_name, coll_name = callback_query.data.split(":")
    mongo_client = user_sessions[callback_query.from_user.id]["mongo_client"]
    deleted_count = await delete_all_documents(mongo_client, db_name, coll_name)
    await callback_query.edit_message_text(f"Deleted {deleted_count} documents from {db_name}.{coll_name}.")

@app.on_callback_query(filters.regex("^main_menu$"))
async def back_to_main_menu(client, callback_query: CallbackQuery):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Manage Databases", callback_data="manage_databases")],
        [InlineKeyboardButton("Manage Collections", callback_data="manage_collections")],
        [InlineKeyboardButton("Manage Documents", callback_data="manage_documents")]
    ])
    await callback_query.edit_message_text("Please select an option:", reply_markup=keyboard)

async def handle(request):
    return web.Response(text="Bot is running")

async def web_server():
    web_app = web.Application()
    web_app.router.add_get("/", handle)
    return web_app

async def main():
    await app.start()

    # Start web server
    port = int(os.environ.get("PORT", 8080))
    web_app = await web_server()
    web_runner = web.AppRunner(web_app)
    await web_runner.setup()
    site = web.TCPSite(web_runner, "0.0.0.0", port)
    await site.start()

    print("Bot started")
    await asyncio.Future()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        loop.run_until_complete(app.stop())
        loop.close()

